package io.plainround.seatwave.partitions.ids;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A clock for demonstrations and tests that returns a scripted list of readings, one per read, and
 * after the script moves forward one millisecond per read, as real time would while a caller waits.
 * A script can step backwards, which is the point. Thread-safe.
 */
public final class SteppedClock extends Clock {

  private final long[] scriptedMillis;
  private final AtomicInteger readCount = new AtomicInteger();

  /**
   * Creates a clock.
   *
   * @param scriptedMillis the readings, in epoch milliseconds, in the order they are returned; at
   *     least one
   * @throws IllegalArgumentException if no reading is given
   */
  public SteppedClock(long... scriptedMillis) {
    if (scriptedMillis.length == 0) {
      throw new IllegalArgumentException("a stepped clock needs at least one reading");
    }
    this.scriptedMillis = scriptedMillis.clone();
  }

  @Override
  public long millis() {
    int readIndex = readCount.getAndIncrement();
    if (readIndex < scriptedMillis.length) {
      return scriptedMillis[readIndex];
    }
    // After the script, time moves on by one millisecond per read.
    return scriptedMillis[scriptedMillis.length - 1] + (readIndex - scriptedMillis.length + 1);
  }

  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(millis());
  }

  @Override
  public ZoneId getZone() {
    return ZoneOffset.UTC;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    if (!ZoneOffset.UTC.equals(zone)) {
      throw new UnsupportedOperationException("a stepped clock reads UTC only");
    }
    return this;
  }

  /** Returns how many times the clock has been read. */
  public int readCount() {
    return readCount.get();
  }
}
