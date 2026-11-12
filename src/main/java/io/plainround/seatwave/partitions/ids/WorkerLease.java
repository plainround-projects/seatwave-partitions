package io.plainround.seatwave.partitions.ids;

import java.time.Instant;
import java.util.Objects;

/**
 * A worker id granted by the directory to one holder, and the instant until which the holder may
 * use it by its own clock. Immutable.
 *
 * @param workerId the granted id, from 0 to 1,023 for {@link IdLayout#seatWave()}
 * @param holder who holds it, for example an instance name
 * @param validUntil the last instant, by the holder's clock, at which it may still issue ids; set
 *     before the directory's own expiry, so the holder always stops first
 */
public record WorkerLease(int workerId, String holder, Instant validUntil) {

  /** Checks that nothing is missing and the id is not negative. */
  public WorkerLease {
    Objects.requireNonNull(holder, "holder");
    Objects.requireNonNull(validUntil, "validUntil");
    if (workerId < 0) {
      throw new IllegalArgumentException("worker id must not be negative, was " + workerId);
    }
  }
}
