package io.plainround.seatwave.partitions.ring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * A consistent-hash ring with virtual nodes: each shard owns {@code virtualNodesPerShard} points on
 * a ring of 2^64 tokens, and a key belongs to the shard of the first point at or after the key's
 * token, wrapping past the largest token to the smallest.
 *
 * <p>The ring is immutable. {@link #withShard} and {@link #withoutShard} return a new ring, so a
 * router can swap rings atomically and never sees a half-built one. Thread-safe by immutability.
 */
public final class HashRing {

  /** The largest number of points one shard may own; more only costs memory and build time. */
  public static final int MAX_VIRTUAL_NODES_PER_SHARD = 4_096;

  // Shard names become part of every point's token, so they are restricted to a stable alphabet.
  private static final Pattern SHARD_NAME = Pattern.compile("[a-z0-9-]{1,32}");
  private static final String POINT_SEPARATOR = "#";
  private static final double TOKENS_ON_THE_RING = 0x1p64;

  private final List<String> shards;
  private final int virtualNodesPerShard;
  private final NavigableMap<Long, String> shardByPoint;

  private HashRing(List<String> shards, int virtualNodesPerShard) {
    this.shards = List.copyOf(shards);
    this.virtualNodesPerShard = virtualNodesPerShard;
    TreeMap<Long, String> points = new TreeMap<>();
    for (String shard : this.shards) {
      for (int pointIndex = 0; pointIndex < virtualNodesPerShard; pointIndex++) {
        // The point's token depends only on the shard's name and the point's number, so every
        // router that knows the shard list builds exactly the same ring, with no coordination.
        long pointToken = TokenHash.tokenOf(shard + POINT_SEPARATOR + pointIndex);
        String earlierOwner = points.putIfAbsent(pointToken, shard);
        if (earlierOwner != null) {
          throw new IllegalStateException(
              "two ring points share token " + pointToken + ": " + earlierOwner + " and " + shard);
        }
      }
    }
    this.shardByPoint = Collections.unmodifiableNavigableMap(points);
  }

  /**
   * Builds a ring.
   *
   * @param shards the shard names, at least one, each matching {@code [a-z0-9-]{1,32}}, no
   *     duplicates
   * @param virtualNodesPerShard points per shard, from 1 to {@value #MAX_VIRTUAL_NODES_PER_SHARD}
   * @return the ring
   * @throws IllegalArgumentException if a shard name or the point count is invalid
   */
  public static HashRing of(List<String> shards, int virtualNodesPerShard) {
    Objects.requireNonNull(shards, "shards");
    if (shards.isEmpty()) {
      throw new IllegalArgumentException("a ring needs at least one shard");
    }
    if (virtualNodesPerShard < 1 || virtualNodesPerShard > MAX_VIRTUAL_NODES_PER_SHARD) {
      throw new IllegalArgumentException(
          "virtual nodes per shard must be 1 to "
              + MAX_VIRTUAL_NODES_PER_SHARD
              + ", was "
              + virtualNodesPerShard);
    }
    HashSet<String> seen = new HashSet<>();
    for (String shard : shards) {
      Objects.requireNonNull(shard, "shard name");
      if (!SHARD_NAME.matcher(shard).matches()) {
        throw new IllegalArgumentException("shard name must match [a-z0-9-]{1,32}: " + shard);
      }
      if (!seen.add(shard)) {
        throw new IllegalArgumentException("shard listed twice: " + shard);
      }
    }
    return new HashRing(shards, virtualNodesPerShard);
  }

  /**
   * Returns a new ring with one more shard and the same points per shard.
   *
   * @throws IllegalArgumentException if the name is invalid or already on the ring
   */
  public HashRing withShard(String shard) {
    List<String> grown = new ArrayList<>(shards);
    grown.add(shard);
    return of(grown, virtualNodesPerShard);
  }

  /**
   * Returns a new ring without one shard.
   *
   * @throws IllegalArgumentException if the shard is not on the ring or is the last one
   */
  public HashRing withoutShard(String shard) {
    if (!shards.contains(shard)) {
      throw new IllegalArgumentException("not on the ring: " + shard);
    }
    List<String> shrunk = new ArrayList<>(shards);
    shrunk.remove(shard);
    return of(shrunk, virtualNodesPerShard);
  }

  /** Returns the shard that owns a key. */
  public String shardFor(String key) {
    return shardForToken(TokenHash.tokenOf(key));
  }

  /** Returns the shard that owns a token: the owner of the first point at or after it. */
  public String shardForToken(long token) {
    // Past the largest point the ring wraps around to the smallest one.
    return Optional.ofNullable(shardByPoint.ceilingEntry(token))
        .orElseGet(shardByPoint::firstEntry)
        .getValue();
  }

  /**
   * Returns each shard's share of the ring's length: the sum of the arcs that end at its points.
   * The shares add up to 1. A key's chance of landing on a shard is that shard's share.
   */
  public Map<String, Double> ringShares() {
    TreeMap<String, Double> shares = new TreeMap<>();
    shards.forEach(shard -> shares.put(shard, 0.0));
    if (shardByPoint.size() == 1) {
      shares.put(shardByPoint.firstEntry().getValue(), 1.0);
      return Collections.unmodifiableMap(shares);
    }
    long previousPoint = shardByPoint.lastKey();
    for (Map.Entry<Long, String> point : shardByPoint.entrySet()) {
      // The subtraction wraps modulo 2^64, which is exactly the arc length on a ring.
      long arcLength = point.getKey() - previousPoint;
      shares.merge(point.getValue(), unsignedToDouble(arcLength) / TOKENS_ON_THE_RING, Double::sum);
      previousPoint = point.getKey();
    }
    return Collections.unmodifiableMap(shares);
  }

  /** Returns the shard names, in the order they were added. */
  public List<String> shards() {
    return shards;
  }

  /** Returns the number of points each shard owns. */
  public int virtualNodesPerShard() {
    return virtualNodesPerShard;
  }

  /** Returns the number of points on the ring. */
  public int pointCount() {
    return shardByPoint.size();
  }

  private static double unsignedToDouble(long value) {
    // A long read as unsigned: halve it to make it positive, convert, double back, add the bit.
    return (value >>> 1) * 2.0 + (value & 1L);
  }
}
