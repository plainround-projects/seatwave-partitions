package io.plainround.seatwave.partitions.ring;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * How many keys each shard of a ring holds, and the skew: the largest shard divided by the average
 * one. A skew of 1.0 is a perfect spread; the module's alert fires above 1.5. Immutable.
 *
 * @param keysPerShard keys per shard name; every shard of the ring appears, with zero if empty
 */
public record KeyDistribution(Map<String, Long> keysPerShard) {

  /** Copies the counts and rejects an empty or negative distribution. */
  public KeyDistribution {
    Objects.requireNonNull(keysPerShard, "keysPerShard");
    if (keysPerShard.isEmpty()) {
      throw new IllegalArgumentException("a distribution needs at least one shard");
    }
    if (keysPerShard.values().stream().anyMatch(count -> count < 0)) {
      throw new IllegalArgumentException("key counts cannot be negative");
    }
    keysPerShard = Collections.unmodifiableMap(new TreeMap<>(keysPerShard));
  }

  /**
   * Places every token on the ring and counts the keys per shard.
   *
   * @param ring the ring
   * @param tokens one token per key, computed once by the caller and reused across rings
   * @return the distribution
   */
  public static KeyDistribution measure(HashRing ring, long[] tokens) {
    TreeMap<String, Long> counts = new TreeMap<>();
    ring.shards().forEach(shard -> counts.put(shard, 0L));
    for (long token : tokens) {
      counts.merge(ring.shardForToken(token), 1L, Long::sum);
    }
    return new KeyDistribution(counts);
  }

  /** Returns the number of keys counted. */
  public long totalKeys() {
    return keysPerShard.values().stream().mapToLong(Long::longValue).sum();
  }

  /** Returns the average number of keys per shard. */
  public double averagePerShard() {
    return (double) totalKeys() / keysPerShard.size();
  }

  /** Returns the number of keys on the fullest shard. */
  public long largest() {
    return keysPerShard.values().stream().mapToLong(Long::longValue).max().orElse(0L);
  }

  /** Returns the largest shard divided by the average; 1.0 means perfectly even. */
  public double skew() {
    return largest() / averagePerShard();
  }

  /** Returns one shard's share of all keys, from 0 to 1. */
  public double shareOf(String shard) {
    return (double) keysPerShard.getOrDefault(shard, 0L) / totalKeys();
  }
}
