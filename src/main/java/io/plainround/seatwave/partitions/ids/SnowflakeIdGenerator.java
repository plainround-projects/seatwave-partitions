package io.plainround.seatwave.partitions.ids;

import io.plainround.seatwave.partitions.ids.IdGenerationRefusedException.Reason;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Issues Snowflake-style ids for one worker: milliseconds since the epoch, the worker id, and a
 * sequence number within the millisecond.
 *
 * <p>The invariant: no two ids from this generator are equal, and each id is larger than the one
 * before it, even when the clock moves backwards. If the clock reads a time before the last issued
 * timestamp, the generator waits for it to catch up when it is behind by at most the tolerance, and
 * refuses otherwise; it never reuses a timestamp it has already left. Uniqueness across workers
 * needs one more thing the generator cannot check: that no two live generators share a worker id
 * (Section 10's lease).
 *
 * <p>Thread-safe: {@link #nextId()} is synchronized, so concurrent callers are served one at a time
 * and see one increasing sequence. A wait for the clock holds the lock, because no other caller
 * could be served correctly during it either.
 */
public final class SnowflakeIdGenerator {

  /** The largest tolerance accepted: a clock more than a second behind is a fault, not jitter. */
  public static final Duration MAX_BACKWARD_TOLERANCE = Duration.ofSeconds(1);

  private static final long WAIT_STEP_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

  private final IdLayout layout;
  private final long workerId;
  private final Clock clock;
  private final long toleranceMillis;
  private final long epochMillis;

  // Guarded by this.
  private long lastTimestampMillis = -1;
  private long sequence;
  private long refusals;
  private long waitedMillis;

  /**
   * Creates a generator.
   *
   * @param layout the bit layout and epoch
   * @param workerId this worker's id, from 0 to {@code layout.maxWorkerId()}, unique among live
   *     generators
   * @param clock the clock to read; injected so tests can move it backwards
   * @param backwardTolerance how far behind the clock may be and still be waited for: from 1 ms to
   *     {@link #MAX_BACKWARD_TOLERANCE}
   * @throws IllegalArgumentException if the worker id or the tolerance is out of range
   */
  public SnowflakeIdGenerator(
      IdLayout layout, long workerId, Clock clock, Duration backwardTolerance) {
    this.layout = Objects.requireNonNull(layout, "layout");
    this.clock = Objects.requireNonNull(clock, "clock");
    Objects.requireNonNull(backwardTolerance, "backwardTolerance");
    if (workerId < 0 || workerId > layout.maxWorkerId()) {
      throw new IllegalArgumentException(
          "worker id must be 0 to " + layout.maxWorkerId() + ", was " + workerId);
    }
    if (backwardTolerance.toMillis() < 1
        || backwardTolerance.compareTo(MAX_BACKWARD_TOLERANCE) > 0) {
      throw new IllegalArgumentException(
          "backward tolerance must be 1 ms to 1 s, was " + backwardTolerance);
    }
    this.workerId = workerId;
    this.toleranceMillis = backwardTolerance.toMillis();
    this.epochMillis = layout.epoch().toEpochMilli();
  }

  /**
   * Returns the next id: unique for this worker and larger than every id it issued before.
   *
   * @throws IdGenerationRefusedException if the clock cannot be trusted to give a fresh, larger
   *     timestamp within the tolerance; no id is issued
   */
  public synchronized long nextId() {
    long nowMillis = readClock();
    if (nowMillis < lastTimestampMillis) {
      // The clock went backwards: never issue from a timestamp we already left behind.
      nowMillis = waitUntil(lastTimestampMillis, nowMillis);
    }
    long nextSequence = 0;
    if (nowMillis == lastTimestampMillis) {
      nextSequence = (sequence + 1) & layout.maxSequence();
      if (nextSequence == 0) {
        // All ids of this millisecond are used: the next id must come from the next millisecond.
        nowMillis = waitUntil(lastTimestampMillis + 1, nowMillis);
      }
    }
    // State changes only here, after every wait succeeded: a refusal leaves it as it was, so a
    // refused call can never cause a later call to repeat a sequence number.
    lastTimestampMillis = nowMillis;
    sequence = nextSequence;
    return layout.compose(nowMillis, workerId, nextSequence);
  }

  /** Returns how many calls were refused since the generator was created. */
  public synchronized long refusals() {
    return refusals;
  }

  /** Returns the milliseconds spent waiting for the clock since the generator was created. */
  public synchronized long waitedMillis() {
    return waitedMillis;
  }

  /** Returns this generator's worker id. */
  public long workerId() {
    return workerId;
  }

  private long waitUntil(long targetMillis, long nowMillis) {
    long behindMillis = targetMillis - nowMillis;
    if (behindMillis > toleranceMillis) {
      throw refuse(
          Reason.CLOCK_BEHIND_BEYOND_TOLERANCE,
          behindMillis,
          "clock is " + behindMillis + " ms behind the last id, tolerance " + toleranceMillis);
    }
    long waitedHere = 0;
    long currentMillis = nowMillis;
    while (currentMillis < targetMillis) {
      // The wait is bounded: a clock that does not advance must not hang every caller.
      if (waitedHere >= toleranceMillis) {
        throw refuse(
            Reason.CLOCK_DID_NOT_CATCH_UP,
            targetMillis - currentMillis,
            "clock did not catch up within " + toleranceMillis + " ms");
      }
      LockSupport.parkNanos(WAIT_STEP_NANOS);
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw refuse(Reason.INTERRUPTED, targetMillis - currentMillis, "interrupted while waiting");
      }
      waitedHere++;
      waitedMillis++;
      currentMillis = readClock();
    }
    return currentMillis;
  }

  private long readClock() {
    long timestampMillis = clock.millis() - epochMillis;
    if (timestampMillis < 0) {
      throw refuse(Reason.CLOCK_BEFORE_EPOCH, 0, "clock reads a time before the layout's epoch");
    }
    if (timestampMillis > layout.maxTimestampMillis()) {
      throw refuse(Reason.TIMESTAMP_BITS_EXHAUSTED, 0, "clock is past the layout's last instant");
    }
    return timestampMillis;
  }

  private IdGenerationRefusedException refuse(Reason reason, long behindMillis, String message) {
    refusals++;
    return new IdGenerationRefusedException(reason, behindMillis, message);
  }
}
