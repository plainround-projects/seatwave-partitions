package io.plainround.seatwave.partitions.routing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class ShardAddressTest {

  @Test
  void aShardLivesInTheSchemaWithItsHyphenReplaced() {
    ShardAddress address = new ShardAddress(1, "shard-17", "shard-host-1");

    assertThat(address.schemaName()).isEqualTo("shard_17");
  }

  @Test
  void refusesAShardNameOutsideTheRingsAlphabet() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new ShardAddress(1, "shard_17; DROP", "shard-host-1"));
  }

  @Test
  void keysAreTheTextsTheRingHashes() {
    UUID holdId = UUID.fromString("019a7b2e-0000-7000-8000-000000000001");

    assertThat(ShardKeys.section(1, "A")).isEqualTo("event-1/section-A");
    assertThat(ShardKeys.customer(42)).isEqualTo("customer-42");
    assertThat(ShardKeys.route(holdId)).isEqualTo("019a7b2e-0000-7000-8000-000000000001");
  }
}
