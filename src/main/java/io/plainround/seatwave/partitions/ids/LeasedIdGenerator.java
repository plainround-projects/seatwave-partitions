package io.plainround.seatwave.partitions.ids;

import io.plainround.seatwave.partitions.ids.IdGenerationRefusedException.Reason;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * A {@link SnowflakeIdGenerator} that issues ids only while its worker-id lease is valid. If the
 * holder cannot renew in time, another instance may take the same worker id after the directory's
 * expiry, so this one must stop first: {@link #nextId()} refuses from the lease's {@code
 * validUntil} on, which {@link WorkerLeases} sets before the directory's own expiry.
 *
 * <p>Thread-safe: the generator is; the lease is replaced whole through a volatile reference.
 */
public final class LeasedIdGenerator {

  private final SnowflakeIdGenerator generator;
  private final Clock clock;
  private volatile WorkerLease lease;

  /**
   * Creates a generator for a lease.
   *
   * @param layout the bit layout and epoch
   * @param lease the lease whose worker id this generator uses
   * @param clock the clock for ids and for the lease's validity
   * @param backwardTolerance as for {@link SnowflakeIdGenerator}
   */
  public LeasedIdGenerator(
      IdLayout layout, WorkerLease lease, Clock clock, Duration backwardTolerance) {
    this.lease = Objects.requireNonNull(lease, "lease");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.generator = new SnowflakeIdGenerator(layout, lease.workerId(), clock, backwardTolerance);
  }

  /**
   * Returns the next id, as {@link SnowflakeIdGenerator#nextId()} does.
   *
   * @throws IdGenerationRefusedException with {@link Reason#WORKER_LEASE_EXPIRED} once the lease is
   *     no longer valid, or for any reason the generator refuses
   */
  public long nextId() {
    WorkerLease current = lease;
    if (!clock.instant().isBefore(current.validUntil())) {
      throw new IdGenerationRefusedException(
          Reason.WORKER_LEASE_EXPIRED,
          0,
          "the lease of worker id "
              + current.workerId()
              + " ran out at "
              + current.validUntil()
              + " and was not renewed");
    }
    return generator.nextId();
  }

  /**
   * Replaces the lease with its renewal.
   *
   * @param renewed the renewed lease, from {@link WorkerLeases#renew(WorkerLease)}
   * @throws IllegalArgumentException if it is for another worker id
   */
  public void renewed(WorkerLease renewed) {
    Objects.requireNonNull(renewed, "renewed");
    if (renewed.workerId() != generator.workerId()) {
      throw new IllegalArgumentException(
          "a renewal for worker id "
              + renewed.workerId()
              + " cannot extend worker id "
              + generator.workerId());
    }
    lease = renewed;
  }
}
