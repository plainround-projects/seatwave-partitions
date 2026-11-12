package io.plainround.seatwave.partitions.ring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

/** Pins the property consistent hashing exists for: a new shard takes keys, nobody else trades. */
class HashRingTest {

  private static final List<String> THREE_SHARDS = List.of("shard-a", "shard-b", "shard-c");
  private static final int VIRTUAL_NODES = 256;
  private static final int KEYS = 100_000;
  // Floating-point sums of 768 arcs are exact to far better than this.
  private static final double RING_SHARE_TOLERANCE = 1e-9;

  @Test
  void addingAShardMovesKeysOnlyToTheNewShard() {
    HashRing before = HashRing.of(THREE_SHARDS, VIRTUAL_NODES);
    HashRing after = before.withShard("shard-d");

    List<String> keysThatMoved =
        keys().filter(key -> !before.shardFor(key).equals(after.shardFor(key))).toList();

    assertThat(keysThatMoved).isNotEmpty();
    assertThat(keysThatMoved).allMatch(key -> after.shardFor(key).equals("shard-d"));
  }

  @Test
  void removingTheAddedShardReturnsEveryKeyToItsFormerShard() {
    HashRing before = HashRing.of(THREE_SHARDS, VIRTUAL_NODES);
    HashRing restored = before.withShard("shard-d").withoutShard("shard-d");

    assertThat(keys()).allMatch(key -> before.shardFor(key).equals(restored.shardFor(key)));
  }

  @Test
  void ringSharesAddUpToTheWholeRing() {
    HashRing ring = HashRing.of(THREE_SHARDS, VIRTUAL_NODES);

    double total = ring.ringShares().values().stream().mapToDouble(Double::doubleValue).sum();

    assertThat(total).isCloseTo(1.0, Offset.offset(RING_SHARE_TOLERANCE));
  }

  @Test
  void rejectsAShardListedTwice() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> HashRing.of(List.of("shard-a", "shard-a"), VIRTUAL_NODES))
        .withMessageContaining("shard listed twice");
  }

  private static Stream<String> keys() {
    return IntStream.rangeClosed(1, KEYS).mapToObj(keyNumber -> "customer-" + keyNumber);
  }
}
