package io.plainround.seatwave.partitions.ids;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Shows SeatWave's Snowflake-style ids: what three layouts can hold, how one id decodes, a run of
 * 4,000,000 ids from 16 threads on 4 workers checked for uniqueness and order, and what the
 * generator does when the clock moves backwards, against a generator that trusts the clock. The
 * clock scenarios use {@link SteppedClock}, so they print the same lines on every run. Run with
 * {@code ./gradlew idGenerator}.
 */
public final class IdGeneratorCli {

  private static final Instant SCENARIO_TIME = Instant.parse("2026-11-11T12:00:00Z");
  private static final long DEMO_WORKER = 7;
  // SeatWave waits out a backward step of up to 10 ms and refuses anything larger (Section 8).
  private static final Duration TOLERANCE = Duration.ofMillis(10);
  private static final int WORKERS = 4;
  private static final int THREADS_PER_WORKER = 4;
  private static final int IDS_PER_THREAD = 250_000;
  private static final long SMALL_STEP_BACK_MILLIS = 5;
  private static final long LARGE_STEP_BACK_MILLIS = 50;
  // The guarded generator gives up only after this many refusals in the demonstration.
  private static final int MAX_DEMO_CALLS = 100;
  private static final long MILLIS_PER_SECOND = 1_000L;
  private static final int SIGN_BITS = 1;
  // Two alternatives to SeatWave's 41/10/12: more years for fewer workers, more workers for fewer
  // years and fewer ids per millisecond.
  private static final IdLayout LONG_LIFE_LAYOUT = new IdLayout(43, 8, 12, IdLayout.SEATWAVE_EPOCH);
  private static final IdLayout MANY_WORKERS_LAYOUT =
      new IdLayout(39, 14, 10, IdLayout.SEATWAVE_EPOCH);
  // Parts 4 and 5 start with ids issued normally before the clock steps back.
  private static final int CALLS_BEFORE_THE_STEP = 2;

  private IdGeneratorCli() {}

  /**
   * Prints the four parts.
   *
   * @throws InterruptedException if interrupted while the threads run
   * @throws ExecutionException if a generating thread failed
   */
  public static void main(String[] arguments) throws InterruptedException, ExecutionException {
    printLayouts();
    printOneDecodedId();
    printThreadedRun();
    printSmallStepBack();
    printLargeStepBack();
    printTrustingGeneratorAgainstGuarded();
  }

  private static void printLayouts() {
    System.out.printf(Locale.ROOT, "PART 1. What a layout can hold (63 bits after the sign bit)%n");
    System.out.printf(
        Locale.ROOT,
        "  %-24s %8s %12s %14s %8s  %s%n",
        "layout (time/worker/seq)",
        "workers",
        "ids/ms/worker",
        "ids/s/worker",
        "years",
        "last instant");
    List<IdLayout> layouts = List.of(IdLayout.seatWave(), LONG_LIFE_LAYOUT, MANY_WORKERS_LAYOUT);
    for (IdLayout layout : layouts) {
      System.out.printf(
          Locale.ROOT,
          "  %-24s %,8d %,12d %,14d %8.1f  %s%n",
          layout.timestampBits() + "/" + layout.workerBits() + "/" + layout.sequenceBits(),
          layout.maxWorkerId() + 1,
          layout.idsPerMillisecondPerWorker(),
          layout.idsPerMillisecondPerWorker() * MILLIS_PER_SECOND,
          layout.lifetimeYears(),
          layout.lastInstant());
    }
  }

  private static void printOneDecodedId() {
    IdLayout layout = IdLayout.seatWave();
    SnowflakeIdGenerator generator =
        new SnowflakeIdGenerator(
            layout, DEMO_WORKER, Clock.fixed(SCENARIO_TIME, ZoneOffset.UTC), TOLERANCE);
    long first = generator.nextId();
    long second = generator.nextId();
    System.out.printf(
        Locale.ROOT, "%nPART 2. Two ids from worker %d at %s%n", DEMO_WORKER, SCENARIO_TIME);
    for (long id : new long[] {first, second}) {
      System.out.printf(
          Locale.ROOT,
          "  id %d = time %d ms after %s + worker %d + sequence %d%n    bits %s%n",
          id,
          layout.timestampOf(id),
          layout.epoch(),
          layout.workerOf(id),
          layout.sequenceOf(id),
          groupedBits(id, layout));
    }
  }

  private static void printThreadedRun() throws InterruptedException, ExecutionException {
    IdLayout layout = IdLayout.seatWave();
    List<SnowflakeIdGenerator> generators = new ArrayList<>();
    for (int workerIndex = 0; workerIndex < WORKERS; workerIndex++) {
      generators.add(
          new SnowflakeIdGenerator(layout, workerIndex + 1, Clock.systemUTC(), TOLERANCE));
    }
    List<Future<long[]>> results = new ArrayList<>();
    // CPU-bound work, so a fixed pool of platform threads, closed by try-with-resources.
    try (ExecutorService pool = Executors.newFixedThreadPool(WORKERS * THREADS_PER_WORKER)) {
      for (SnowflakeIdGenerator generator : generators) {
        for (int threadIndex = 0; threadIndex < THREADS_PER_WORKER; threadIndex++) {
          results.add(pool.submit(() -> idsFrom(generator)));
        }
      }
    }
    long[] everyId = new long[WORKERS * THREADS_PER_WORKER * IDS_PER_THREAD];
    int filled = 0;
    int threadsInOrder = 0;
    for (Future<long[]> result : results) {
      long[] idsOfThread = result.get();
      if (isStrictlyIncreasing(idsOfThread)) {
        threadsInOrder++;
      }
      System.arraycopy(idsOfThread, 0, everyId, filled, idsOfThread.length);
      filled += idsOfThread.length;
    }
    // Sorting and comparing neighbours finds any duplicate with 32 MB, where a set would need GBs.
    Arrays.sort(everyId);
    long duplicates = 0;
    for (int index = 1; index < everyId.length; index++) {
      if (everyId[index] == everyId[index - 1]) {
        duplicates++;
      }
    }
    long refusals = generators.stream().mapToLong(SnowflakeIdGenerator::refusals).sum();
    System.out.printf(
        Locale.ROOT,
        "%nPART 3. %d workers x %d threads x %,d ids on the system clock%n"
            + "  ids issued %,d, duplicates %d, refusals %d%n"
            + "  threads whose ids strictly increase %d of %d%n",
        WORKERS,
        THREADS_PER_WORKER,
        IDS_PER_THREAD,
        everyId.length,
        duplicates,
        refusals,
        threadsInOrder,
        results.size());
  }

  private static void printSmallStepBack() {
    long base = SCENARIO_TIME.toEpochMilli();
    long[] readings = {base, base, base + 1, base + 1 - SMALL_STEP_BACK_MILLIS};
    SteppedClock clock = new SteppedClock(readings);
    SnowflakeIdGenerator generator =
        new SnowflakeIdGenerator(IdLayout.seatWave(), DEMO_WORKER, clock, TOLERANCE);
    System.out.printf(
        Locale.ROOT,
        "%nPART 4. The clock steps back %d ms (tolerance %d ms): the generator waits%n",
        SMALL_STEP_BACK_MILLIS,
        TOLERANCE.toMillis());
    long previous = -1;
    for (int call = 1; call <= readings.length; call++) {
      long id = generator.nextId();
      printIssued(call, id, base, previous, generator);
      previous = id;
    }
  }

  private static void printLargeStepBack() {
    long base = SCENARIO_TIME.toEpochMilli();
    SteppedClock clock = new SteppedClock(base, base + 1, base + 1 - LARGE_STEP_BACK_MILLIS);
    SnowflakeIdGenerator generator =
        new SnowflakeIdGenerator(IdLayout.seatWave(), DEMO_WORKER, clock, TOLERANCE);
    System.out.printf(
        Locale.ROOT,
        "%nPART 5. The clock steps back %d ms (tolerance %d ms): the generator refuses%n",
        LARGE_STEP_BACK_MILLIS,
        TOLERANCE.toMillis());
    long previous = -1;
    for (int call = 1; call <= MAX_DEMO_CALLS; call++) {
      try {
        long id = generator.nextId();
        printIssued(call, id, base, previous, generator);
        if (call > CALLS_BEFORE_THE_STEP) {
          return;
        }
        previous = id;
      } catch (IdGenerationRefusedException refused) {
        // Print the first refusal and the last one before the clock was close enough to wait for.
        long behind = refused.clockBehindMillis();
        if (generator.refusals() == 1 || behind == TOLERANCE.toMillis() + 1) {
          System.out.printf(
              Locale.ROOT,
              "  call %3d: REFUSED %s, clock %d ms behind (refusals so far %d)%n",
              call,
              refused.reason(),
              behind,
              generator.refusals());
        }
      }
    }
  }

  private static void printTrustingGeneratorAgainstGuarded() {
    long base = SCENARIO_TIME.toEpochMilli();
    long[] readings = {base, base + 1, base};
    IdLayout layout = IdLayout.seatWave();
    System.out.printf(
        Locale.ROOT, "%nPART 6. Clock readings +0, +1, +0 ms: a generator that trusts the clock%n");
    long lastTimestamp = -1;
    long sequence = 0;
    List<Long> issued = new ArrayList<>();
    for (int call = 1; call <= readings.length; call++) {
      long timestamp = readings[call - 1] - layout.epoch().toEpochMilli();
      // The trusting rule: same millisecond, next sequence; any other millisecond, sequence 0.
      sequence = timestamp == lastTimestamp ? sequence + 1 : 0;
      lastTimestamp = timestamp;
      long id = layout.compose(timestamp, DEMO_WORKER, sequence);
      System.out.printf(
          Locale.ROOT,
          "  call %d: id %d (time +%d ms, sequence %d)%s%n",
          call,
          id,
          timestamp - (base - layout.epoch().toEpochMilli()),
          sequence,
          issued.contains(id) ? "  DUPLICATE of call " + (issued.indexOf(id) + 1) : "");
      issued.add(id);
    }
    System.out.printf(Locale.ROOT, "  the guarded generator on the same readings:%n");
    SnowflakeIdGenerator guarded =
        new SnowflakeIdGenerator(layout, DEMO_WORKER, new SteppedClock(readings), TOLERANCE);
    long previous = -1;
    for (int call = 1; call <= readings.length; call++) {
      long id = guarded.nextId();
      printIssued(call, id, base, previous, guarded);
      previous = id;
    }
  }

  private static long[] idsFrom(SnowflakeIdGenerator generator) {
    long[] ids = new long[IDS_PER_THREAD];
    for (int index = 0; index < IDS_PER_THREAD; index++) {
      ids[index] = generator.nextId();
    }
    return ids;
  }

  private static boolean isStrictlyIncreasing(long[] ids) {
    for (int index = 1; index < ids.length; index++) {
      if (ids[index] <= ids[index - 1]) {
        return false;
      }
    }
    return true;
  }

  private static void printIssued(
      int call, long id, long baseMillis, long previous, SnowflakeIdGenerator generator) {
    IdLayout layout = IdLayout.seatWave();
    long offset = layout.timestampOf(id) - (baseMillis - layout.epoch().toEpochMilli());
    System.out.printf(
        Locale.ROOT,
        "  call %3d: id %d (time +%d ms, sequence %d)%s, waited %d ms in all%n",
        call,
        id,
        offset,
        layout.sequenceOf(id),
        previous < 0 ? "" : id > previous ? ", larger than the last" : ", NOT LARGER",
        generator.waitedMillis());
  }

  private static String groupedBits(long id, IdLayout layout) {
    String bits = String.format(Locale.ROOT, "%64s", Long.toBinaryString(id)).replace(' ', '0');
    int timeEnd = SIGN_BITS + layout.timestampBits();
    int workerEnd = timeEnd + layout.workerBits();
    return bits.substring(0, SIGN_BITS)
        + " | "
        + bits.substring(SIGN_BITS, timeEnd)
        + " | "
        + bits.substring(timeEnd, workerEnd)
        + " | "
        + bits.substring(workerEnd);
  }
}
