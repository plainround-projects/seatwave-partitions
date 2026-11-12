package io.plainround.seatwave.partitions.shardkey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.plainround.seatwave.partitions.ring.HashRing;
import java.util.List;
import org.junit.jupiter.api.Test;

class CandidateKeyTest {

  private static final int POINTS = 64;
  private static final long EVENT = 1;
  private static final long CUSTOMER = 42;
  private static final long BOOKING = 7;
  private static final HashRing RING =
      HashRing.of(List.of("shard-a", "shard-b", "shard-c"), POINTS);

  @Test
  void eventSectionRoutesBySectionSoOneSeatsClaimsShareAShard() {
    BookingWrite first = new BookingWrite(EVENT, "A", CUSTOMER, BOOKING, 0);
    BookingWrite second = new BookingWrite(EVENT, "A", CUSTOMER + 1, BOOKING + 1, 1);

    assertThat(CandidateKey.EVENT_SECTION.shardOf(first, RING))
        .isEqualTo(RING.shardFor("event-1/section-A"))
        .isEqualTo(CandidateKey.EVENT_SECTION.shardOf(second, RING));
  }

  @Test
  void bookingMonthMapsTheLastMonthToTheLastShardByPosition() {
    BookingWrite lastMonth =
        new BookingWrite(EVENT, "A", CUSTOMER, BOOKING, CandidateKey.PLANNED_MONTHS - 1);

    assertThat(CandidateKey.BOOKING_MONTH.shardOf(lastMonth, RING))
        .isEqualTo(RING.shards().getLast());
  }

  @Test
  void onlyKeysDerivedFromTheSeatKeepTheSeatRule() {
    assertThat(List.of(CandidateKey.values()))
        .filteredOn(CandidateKey::keepsSeatRuleOnOneShard)
        .containsExactly(CandidateKey.EVENT, CandidateKey.EVENT_SECTION);
    assertThat(CandidateKey.yesOrNo(CandidateKey.CUSTOMER.keepsHoldLimitOnOneShard()))
        .isEqualTo("yes");
  }

  @Test
  void aBookingWriteRefusesAMalformedSection() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new BookingWrite(EVENT, "a1", CUSTOMER, BOOKING, 0));
  }
}
