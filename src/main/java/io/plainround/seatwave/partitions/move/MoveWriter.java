package io.plainround.seatwave.partitions.move;

import com.github.sviperll.result4j.Result;
import io.plainround.seatwave.partitions.holds.HoldFlow;
import io.plainround.seatwave.partitions.lab.LabCluster;
import io.plainround.seatwave.partitions.lab.SectionKey;
import io.plainround.seatwave.partitions.routing.WriteRefusal;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Sends holds and their confirms on the moving shard's free seats at a fixed rate while a move
 * runs, open loop: one attempt per tick, each on its own virtual thread, whatever the previous ones
 * are doing, so a paused shard queues writes instead of slowing the sender. Each attempt takes the
 * next free seat in order and the next customer in turn; it holds the seat and, if that succeeds,
 * confirms it. It records every acknowledged hold and booking id and every refusal by kind, so the
 * audit can check that each acknowledged write is on the new machine exactly once.
 *
 * <p>Thread-safe: the counters and queues are concurrent; start and stop run on one thread.
 */
public final class MoveWriter implements AutoCloseable {

  private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

  private final HoldFlow flow;
  private final List<Long> customers;
  private final List<SectionKey> sections;
  private final long periodNanos;
  private final long maxAttempts;
  private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
  private final ExecutorService attempts = Executors.newVirtualThreadPerTaskExecutor();
  private final AtomicLong sent = new AtomicLong();
  private final Queue<UUID> heldIds = new ConcurrentLinkedQueue<>();
  private final Queue<UUID> bookedIds = new ConcurrentLinkedQueue<>();
  private final Map<String, LongAdder> refusals = new ConcurrentHashMap<>();
  private final LongAdder failures = new LongAdder();

  /**
   * Creates a writer; nothing is sent until {@link #start()}.
   *
   * @param flow the hold flow
   * @param customers registered customers, used in turn
   * @param sections the moving shard's sections; their seats above the seed's are used in order
   * @param holdsPerSecond the sending rate
   */
  public MoveWriter(
      HoldFlow flow, List<Long> customers, List<SectionKey> sections, int holdsPerSecond) {
    this.flow = Objects.requireNonNull(flow, "flow");
    this.customers = List.copyOf(customers);
    this.sections = List.copyOf(sections);
    if (this.customers.isEmpty() || this.sections.isEmpty() || holdsPerSecond < 1) {
      throw new IllegalArgumentException("the writer needs customers, sections and a rate");
    }
    this.periodNanos = NANOS_PER_SECOND / holdsPerSecond;
    // The free seats bound the attempts: one seat per attempt, never the same seat twice.
    this.maxAttempts =
        (long) this.sections.size()
            * (LabCluster.SEATS_PER_SECTION - ShardSeed.BOOKED_SEATS_PER_SECTION);
  }

  /** Starts sending. */
  public void start() {
    ticker.scheduleAtFixedRate(this::sendOne, 0, periodNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Stops sending, waits for every attempt in flight, and returns what happened.
   *
   * @return the tally
   */
  public WriterTally stop() {
    ticker.shutdownNow();
    attempts.close();
    Map<String, Long> refused = new TreeMap<>();
    refusals.forEach((kind, count) -> refused.put(kind, count.sum()));
    return new WriterTally(
        sent.get(), List.copyOf(heldIds), List.copyOf(bookedIds), refused, failures.sum());
  }

  @Override
  public void close() {
    ticker.shutdownNow();
    attempts.close();
  }

  private void sendOne() {
    long attempt = sent.get();
    if (attempt >= maxAttempts) {
      return;
    }
    sent.incrementAndGet();
    SectionKey section = sections.get((int) (attempt % sections.size()));
    int seatNumber = ShardSeed.BOOKED_SEATS_PER_SECTION + 1 + (int) (attempt / sections.size());
    long customerId = customers.get((int) (attempt % customers.size()));
    attempts.execute(() -> holdAndConfirm(customerId, section, seatNumber));
  }

  private void holdAndConfirm(long customerId, SectionKey section, int seatNumber) {
    try {
      Result<UUID, WriteRefusal> held =
          flow.hold(customerId, section.eventId(), section.seatLabel(seatNumber));
      if (!(held instanceof Result.Success<UUID, WriteRefusal>(UUID holdId))) {
        count(held);
        return;
      }
      heldIds.add(holdId);
      Result<UUID, WriteRefusal> booked = flow.confirm(holdId);
      if (booked instanceof Result.Success<UUID, WriteRefusal>(UUID bookingId)) {
        bookedIds.add(bookingId);
      } else {
        count(booked);
      }
    } catch (SQLException failure) {
      // Counted, not hidden: the audit reports every failure, and a failed attempt's outcome is
      // unknown, so the audit's equality checks would show it if it had been written.
      failures.increment();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      failures.increment();
    }
  }

  private void count(Result<UUID, WriteRefusal> refused) {
    if (refused instanceof Result.Error<UUID, WriteRefusal>(WriteRefusal refusal)) {
      refusals
          .computeIfAbsent(refusal.getClass().getSimpleName(), ignored -> new LongAdder())
          .increment();
    }
  }
}
