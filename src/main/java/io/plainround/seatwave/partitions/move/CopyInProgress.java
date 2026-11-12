package io.plainround.seatwave.partitions.move;

import java.time.Duration;
import java.util.Objects;

/**
 * A shard copy that is ready and streaming, between {@link ShardMover#copy} and {@link
 * ShardMover#finish}. Immutable.
 *
 * @param shardName the shard being moved
 * @param fromHost the machine that still owns it
 * @param toHost the machine the copy is on
 * @param sourceTableBytes the shard's tables on the old machine before the copy, without indexes
 * @param sourceTotalBytes the same with indexes
 * @param copy how long the copy took
 */
public record CopyInProgress(
    String shardName,
    String fromHost,
    String toHost,
    long sourceTableBytes,
    long sourceTotalBytes,
    Duration copy) {

  /** Checks that nothing is missing. */
  public CopyInProgress {
    Objects.requireNonNull(shardName, "shardName");
    Objects.requireNonNull(fromHost, "fromHost");
    Objects.requireNonNull(toHost, "toHost");
    Objects.requireNonNull(copy, "copy");
  }
}
