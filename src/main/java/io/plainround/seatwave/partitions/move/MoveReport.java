package io.plainround.seatwave.partitions.move;

import java.time.Duration;
import java.util.Objects;

/**
 * What one shard move measured. Immutable.
 *
 * @param shardName the shard that moved
 * @param fromHost the machine it left
 * @param toHost the machine it serves from now
 * @param ringVersion the ring version, unchanged by a move: no key changed shard
 * @param sourceTableBytes the shard's tables on the old machine before the copy, without indexes
 *     (what the copy reads and sends)
 * @param sourceTotalBytes the same with indexes
 * @param targetTotalBytes the shard's tables with indexes on the new machine after the move
 * @param copy from creating the subscription to every table being ready (copied and streaming)
 * @param catchUp from the end of the copy until a marker written then had arrived
 * @param pause from pausing the shard's writes to letting them through on the new machine
 * @param fence the part of the pause until the old machine stopped accepting writes
 * @param lagZero the part of the pause until the fence's marker had arrived (lag 0)
 */
public record MoveReport(
    String shardName,
    String fromHost,
    String toHost,
    int ringVersion,
    long sourceTableBytes,
    long sourceTotalBytes,
    long targetTotalBytes,
    Duration copy,
    Duration catchUp,
    Duration pause,
    Duration fence,
    Duration lagZero) {

  private static final double NANOS_PER_SECOND = 1e9;

  /** Checks that nothing is missing. */
  public MoveReport {
    Objects.requireNonNull(shardName, "shardName");
    Objects.requireNonNull(fromHost, "fromHost");
    Objects.requireNonNull(toHost, "toHost");
    Objects.requireNonNull(copy, "copy");
    Objects.requireNonNull(catchUp, "catchUp");
    Objects.requireNonNull(pause, "pause");
    Objects.requireNonNull(fence, "fence");
    Objects.requireNonNull(lagZero, "lagZero");
  }

  /** Returns the copy speed: table bytes sent per second of copy. */
  public double copyBytesPerSecond() {
    return sourceTableBytes / (copy.toNanos() / NANOS_PER_SECOND);
  }
}
