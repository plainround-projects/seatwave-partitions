package io.plainround.seatwave.partitions.ids;

import java.io.Serial;

/**
 * Thrown when the generator cannot guarantee that the next id is unique and larger than the last
 * one, so it issues none. This is an infrastructure failure (the clock), not a business outcome, so
 * it is an exception (code guidelines, rule 47); the caller fails safe, for example with a 503.
 */
public final class IdGenerationRefusedException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  /** Why no id was issued. */
  public enum Reason {
    /** The clock is behind the last issued timestamp by more than the tolerance. */
    CLOCK_BEHIND_BEYOND_TOLERANCE,
    /** The generator waited the whole tolerance and the clock still had not caught up. */
    CLOCK_DID_NOT_CATCH_UP,
    /** The clock reads a time before the layout's epoch. */
    CLOCK_BEFORE_EPOCH,
    /** The clock reads a time after the last instant the layout can encode. */
    TIMESTAMP_BITS_EXHAUSTED,
    /** The calling thread was interrupted while it waited for the clock. */
    INTERRUPTED,
    /**
     * The worker-id lease ran out before it was renewed, so the worker id may be someone else's.
     */
    WORKER_LEASE_EXPIRED
  }

  private final Reason reason;
  private final long clockBehindMillis;

  IdGenerationRefusedException(Reason reason, long clockBehindMillis, String message) {
    super(message);
    this.reason = reason;
    this.clockBehindMillis = clockBehindMillis;
  }

  /** Returns why no id was issued. */
  public Reason reason() {
    return reason;
  }

  /** Returns how far the clock was behind the last issued timestamp, 0 if it was not behind. */
  public long clockBehindMillis() {
    return clockBehindMillis;
  }
}
