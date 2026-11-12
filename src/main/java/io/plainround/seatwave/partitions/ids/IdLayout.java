package io.plainround.seatwave.partitions.ids;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * How a 64-bit Snowflake-style id is divided: from the top, one sign bit that is always 0, then
 * milliseconds since the epoch, then the worker id, then the sequence number within one
 * millisecond. Because time comes first, ids sort by creation time. Immutable.
 *
 * @param timestampBits bits for milliseconds since the epoch
 * @param workerBits bits for the worker id
 * @param sequenceBits bits for the sequence number within one millisecond
 * @param epoch the instant whose millisecond is timestamp 0
 */
public record IdLayout(int timestampBits, int workerBits, int sequenceBits, Instant epoch) {

  /**
   * SeatWave's epoch: ids count milliseconds from the start of 2026, so none are wasted on 1970.
   */
  public static final Instant SEATWAVE_EPOCH = Instant.parse("2026-01-01T00:00:00Z");

  // The sign bit stays 0, so ids are positive and sort the same way as signed and unsigned numbers.
  private static final int PAYLOAD_BITS = Long.SIZE - 1;
  // The classic split (Twitter's Snowflake, 2010): 41 bits of time, 10 of machine, 12 of sequence.
  private static final int CLASSIC_TIMESTAMP_BITS = 41;
  private static final int CLASSIC_WORKER_BITS = 10;
  private static final int CLASSIC_SEQUENCE_BITS = 12;
  private static final double DAYS_PER_YEAR = 365.0;

  /** Rejects a layout whose fields do not fill the 63 payload bits exactly. */
  public IdLayout {
    Objects.requireNonNull(epoch, "epoch");
    if (timestampBits < 1 || workerBits < 0 || sequenceBits < 1) {
      throw new IllegalArgumentException(
          "a layout needs at least 1 timestamp bit and 1 sequence bit, and no negative field");
    }
    if (timestampBits + workerBits + sequenceBits != PAYLOAD_BITS) {
      throw new IllegalArgumentException(
          "timestamp, worker and sequence bits must add up to "
              + PAYLOAD_BITS
              + ", were "
              + (timestampBits + workerBits + sequenceBits));
    }
  }

  /** Returns SeatWave's layout: 41 bits of time, 10 of worker, 12 of sequence, from 2026. */
  public static IdLayout seatWave() {
    return new IdLayout(
        CLASSIC_TIMESTAMP_BITS, CLASSIC_WORKER_BITS, CLASSIC_SEQUENCE_BITS, SEATWAVE_EPOCH);
  }

  /** Returns the largest timestamp the layout can hold, in milliseconds since the epoch. */
  public long maxTimestampMillis() {
    return (1L << timestampBits) - 1;
  }

  /** Returns the largest worker id; ids from 0 to this value are valid. */
  public long maxWorkerId() {
    return (1L << workerBits) - 1;
  }

  /** Returns the largest sequence number within one millisecond. */
  public long maxSequence() {
    return (1L << sequenceBits) - 1;
  }

  /** Returns how many ids one worker can issue in one millisecond. */
  public long idsPerMillisecondPerWorker() {
    return maxSequence() + 1;
  }

  /** Returns the last instant the layout can encode; after it the generator must refuse. */
  public Instant lastInstant() {
    return epoch.plusMillis(maxTimestampMillis());
  }

  /** Returns how long the layout lasts, in years of 365 days. */
  public double lifetimeYears() {
    return Duration.ofMillis(maxTimestampMillis()).toDays() / DAYS_PER_YEAR;
  }

  /**
   * Packs the three fields into an id.
   *
   * @throws IllegalArgumentException if a field does not fit its bits
   */
  public long compose(long timestampMillis, long workerId, long sequence) {
    if (timestampMillis < 0 || timestampMillis > maxTimestampMillis()) {
      throw new IllegalArgumentException("timestamp outside the layout: " + timestampMillis);
    }
    if (workerId < 0 || workerId > maxWorkerId()) {
      throw new IllegalArgumentException("worker id outside the layout: " + workerId);
    }
    if (sequence < 0 || sequence > maxSequence()) {
      throw new IllegalArgumentException("sequence outside the layout: " + sequence);
    }
    return (timestampMillis << (workerBits + sequenceBits)) | (workerId << sequenceBits) | sequence;
  }

  /** Returns the id's timestamp, in milliseconds since the epoch. */
  public long timestampOf(long id) {
    return id >>> (workerBits + sequenceBits);
  }

  /** Returns the id's worker id. */
  public long workerOf(long id) {
    return (id >>> sequenceBits) & maxWorkerId();
  }

  /** Returns the id's sequence number. */
  public long sequenceOf(long id) {
    return id & maxSequence();
  }
}
