package io.plainround.seatwave.partitions.ids;

/** Why the directory granted or renewed no lease: expected outcomes, returned as values. */
public enum LeaseRefusal {
  /** Every worker id has a live lease. */
  NO_FREE_WORKER_ID,
  /** The lease expired before the renewal, or another holder has the id now: stop issuing. */
  LEASE_LOST
}
