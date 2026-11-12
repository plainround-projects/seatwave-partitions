package io.plainround.seatwave.partitions.routing;

/**
 * A pause of one shard's writes, from {@link ShardRouter#pause(String)}; closing it lets the queued
 * writes through. Close it once, on the thread that took it.
 */
@FunctionalInterface
public interface ShardPause extends AutoCloseable {

  /** Ends the pause. */
  @Override
  void close();
}
