package io.plainround.seatwave.partitions.ring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KeyDistributionTest {

  private static final int POINTS = 16;

  @Test
  void countsEveryTokenOnceAndListsShardsThatGotNone() {
    HashRing ring = HashRing.of(List.of("shard-a", "shard-b", "shard-c"), POINTS);
    long[] tokens = {TokenHash.tokenOf("customer-1"), TokenHash.tokenOf("customer-2")};

    KeyDistribution distribution = KeyDistribution.measure(ring, tokens);

    assertThat(distribution.totalKeys()).isEqualTo(2);
    assertThat(distribution.keysPerShard()).containsOnlyKeys("shard-a", "shard-b", "shard-c");
  }

  @Test
  void skewIsTheLargestShardOverTheAverage() {
    KeyDistribution distribution = new KeyDistribution(Map.of("shard-a", 6L, "shard-b", 2L));

    assertThat(distribution.skew()).isEqualTo(1.5);
    assertThat(distribution.shareOf("shard-a")).isEqualTo(0.75);
  }

  @Test
  void rejectsANegativeCount() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new KeyDistribution(Map.of("shard-a", -1L)));
  }
}
