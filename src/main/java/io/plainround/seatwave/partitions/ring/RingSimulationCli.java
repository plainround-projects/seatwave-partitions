package io.plainround.seatwave.partitions.ring;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

/**
 * Places 10,000,000 keys on consistent-hash rings and prints, as text charts, the distribution and
 * skew with and without virtual nodes, and how many keys move when a shard is added, against the
 * theory and against modulo hashing. Deterministic: tokens come from MD5 of fixed key names, so
 * every run prints the same numbers. Run with {@code ./gradlew ringSimulation}.
 */
public final class RingSimulationCli {

  // Ten million keys: enough that a shard's count differs from its ring share by well under 0.1
  // percent, small enough to keep every token in memory (80 MB).
  private static final int KEY_COUNT = 10_000_000;
  private static final List<String> THREE_SHARDS = List.of("shard-a", "shard-b", "shard-c");
  private static final String FOURTH_SHARD = "shard-d";
  private static final List<Integer> VIRTUAL_NODE_SETTINGS = List.of(1, 16, 64, 256);
  // The many-points case of Parts 1 and 2, and the first setting tried for SeatWave's 64 shards.
  private static final int MANY_VIRTUAL_NODES = 256;
  // SeatWave's plan (Section 7): 64 shards with 1,024 points each, then a 65th.
  private static final int PLANNED_SHARDS = 64;
  private static final int PLANNED_VIRTUAL_NODES = 1_024;
  private static final int CHART_WIDTH = 40;
  private static final double PERCENT = 100.0;

  private RingSimulationCli() {}

  /** Prints the three parts of the simulation. */
  public static void main(String[] arguments) {
    long[] tokens = tokensOfKeys();
    System.out.printf(
        Locale.ROOT,
        "Consistent-hash ring: %,d keys (customer-1 to customer-%d), token = first 8 bytes of"
            + " MD5%n",
        KEY_COUNT,
        KEY_COUNT);
    printThreeShards(tokens);
    printAddingAFourthShard(tokens);
    printPlannedRing(tokens);
  }

  private static long[] tokensOfKeys() {
    // Tokens are computed once and reused by every ring below; only the ring changes.
    return IntStream.rangeClosed(1, KEY_COUNT)
        .mapToLong(keyNumber -> TokenHash.tokenOf("customer-" + keyNumber))
        .toArray();
  }

  private static void printThreeShards(long[] tokens) {
    System.out.printf(Locale.ROOT, "%nPART 1. Three shards: keys per shard by virtual nodes%n");
    for (int virtualNodes : VIRTUAL_NODE_SETTINGS) {
      HashRing ring = HashRing.of(THREE_SHARDS, virtualNodes);
      printDistribution(ring, KeyDistribution.measure(ring, tokens));
    }
  }

  private static void printAddingAFourthShard(long[] tokens) {
    System.out.printf(
        Locale.ROOT, "%nPART 2. Adding %s: which keys move, and from where%n", FOURTH_SHARD);
    for (int virtualNodes : List.of(1, MANY_VIRTUAL_NODES)) {
      HashRing before = HashRing.of(THREE_SHARDS, virtualNodes);
      HashRing after = before.withShard(FOURTH_SHARD);
      System.out.printf(
          Locale.ROOT, "%n%d virtual node(s) per shard, after the addition:%n", virtualNodes);
      printDistribution(after, KeyDistribution.measure(after, tokens));
      printMovedKeys(before, after, tokens);
    }
    printModuloMoves(tokens, THREE_SHARDS.size());
  }

  private static void printPlannedRing(long[] tokens) {
    System.out.printf(
        Locale.ROOT, "%nPART 3. SeatWave's plan: %d shards, then one more%n", PLANNED_SHARDS);
    List<String> shardNames =
        IntStream.range(0, PLANNED_SHARDS)
            .mapToObj(shardNumber -> String.format(Locale.ROOT, "shard-%02d", shardNumber))
            .toList();
    for (int virtualNodes : List.of(1, MANY_VIRTUAL_NODES, PLANNED_VIRTUAL_NODES)) {
      HashRing before = HashRing.of(shardNames, virtualNodes);
      KeyDistribution distribution = KeyDistribution.measure(before, tokens);
      System.out.printf(
          Locale.ROOT,
          "  %4d virtual node(s): smallest shard %,d keys, largest %,d, average %,.0f, skew %.3f%n",
          virtualNodes,
          distribution.keysPerShard().values().stream().mapToLong(Long::longValue).min().orElse(0L),
          distribution.largest(),
          distribution.averagePerShard(),
          distribution.skew());
      HashRing after = before.withShard(String.format(Locale.ROOT, "shard-%02d", PLANNED_SHARDS));
      printMovedKeys(before, after, tokens);
    }
    printModuloMoves(tokens, PLANNED_SHARDS);
  }

  private static void printDistribution(HashRing ring, KeyDistribution distribution) {
    Map<String, Double> ringShares = ring.ringShares();
    System.out.printf(
        Locale.ROOT,
        "  virtual nodes %d (%d points)   skew %.3f%n",
        ring.virtualNodesPerShard(),
        ring.pointCount(),
        distribution.skew());
    for (Map.Entry<String, Long> shard : distribution.keysPerShard().entrySet()) {
      int barLength =
          (int) Math.round(CHART_WIDTH * (double) shard.getValue() / distribution.largest());
      System.out.printf(
          Locale.ROOT,
          "    %-8s %,11d keys %5.1f%% (ring share %5.1f%%) %s%n",
          shard.getKey(),
          shard.getValue(),
          PERCENT * distribution.shareOf(shard.getKey()),
          PERCENT * ringShares.get(shard.getKey()),
          "#".repeat(barLength));
    }
  }

  private static void printMovedKeys(HashRing before, HashRing after, long[] tokens) {
    TreeMap<String, Long> movedFrom = new TreeMap<>();
    long moved = 0;
    for (long token : tokens) {
      String oldShard = before.shardForToken(token);
      String newShard = after.shardForToken(token);
      if (!oldShard.equals(newShard)) {
        moved++;
        movedFrom.merge(oldShard, 1L, Long::sum);
      }
    }
    int shardsAfter = after.shards().size();
    System.out.printf(
        Locale.ROOT,
        "  moved %,d of %,d keys (%.2f%%); theory 1/(N+1) = 1/%d = %.2f%%; moved keys came from %d"
            + " shard(s)%n",
        moved,
        tokens.length,
        PERCENT * moved / tokens.length,
        shardsAfter,
        PERCENT / shardsAfter,
        movedFrom.size());
    if (movedFrom.size() <= THREE_SHARDS.size()) {
      movedFrom.forEach((shard, count) ->
          System.out.printf(Locale.ROOT, "    from %-8s %,11d keys%n", shard, count));
    }
  }

  private static void printModuloMoves(long[] tokens, int shardsBefore) {
    int shardsAfter = shardsBefore + 1;
    long moved = 0;
    for (long token : tokens) {
      // Modulo hashing: shard number = token mod N, read as unsigned so it is never negative.
      if (Long.remainderUnsigned(token, shardsBefore)
          != Long.remainderUnsigned(token, shardsAfter)) {
        moved++;
      }
    }
    System.out.printf(
        Locale.ROOT,
        "  modulo hashing %d -> %d shards: moved %,d keys (%.2f%%); theory N/(N+1) = %d/%d ="
            + " %.2f%%%n",
        shardsBefore,
        shardsAfter,
        moved,
        PERCENT * moved / tokens.length,
        shardsBefore,
        shardsAfter,
        PERCENT * shardsBefore / shardsAfter);
  }
}
