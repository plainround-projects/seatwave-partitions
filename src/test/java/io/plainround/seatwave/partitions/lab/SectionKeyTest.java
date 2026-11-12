package io.plainround.seatwave.partitions.lab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

class SectionKeyTest {

  @Test
  void namesItsShardKeyAndItsSeats() {
    SectionKey section = new SectionKey(1, "A");

    assertThat(section.shardKey()).isEqualTo("event-1/section-A");
    assertThat(section.seatLabel(7)).isEqualTo("A-7");
  }

  @Test
  void refusesAnEventIdBelowOne() {
    assertThatIllegalArgumentException().isThrownBy(() -> new SectionKey(0, "A"));
  }
}
