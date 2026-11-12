package io.plainround.seatwave.partitions.shardkey;

import io.plainround.seatwave.partitions.ring.HashRing;

/**
 * The five shard keys Section 7 compares for SeatWave's booking data, with the routing each one
 * uses and which of Module 13's two rules it keeps inside one shard.
 */
enum CandidateKey {
  EVENT("event_id", true, false),
  CUSTOMER("customer_id", false, true),
  BOOKING("booking_id", false, false),
  EVENT_SECTION("(event_id, section_code)", true, false),
  BOOKING_MONTH("booking month (range)", false, false);

  /** The months the date ranges cover: 64 ranges of 10 months, about 53 years. */
  static final int PLANNED_MONTHS = 640;

  private final String label;
  private final boolean keepsSeatRuleOnOneShard;
  private final boolean keepsHoldLimitOnOneShard;

  CandidateKey(String label, boolean keepsSeatRuleOnOneShard, boolean keepsHoldLimitOnOneShard) {
    this.label = label;
    this.keepsSeatRuleOnOneShard = keepsSeatRuleOnOneShard;
    this.keepsHoldLimitOnOneShard = keepsHoldLimitOnOneShard;
  }

  /** Returns the shard this key sends a write to. */
  String shardOf(BookingWrite write, HashRing ring) {
    return switch (this) {
      case EVENT -> ring.shardFor("event-" + write.eventId());
      case CUSTOMER -> ring.shardFor("customer-" + write.customerId());
      case BOOKING -> ring.shardFor("booking-" + Long.toHexString(write.bookingId()));
      case EVENT_SECTION ->
          ring.shardFor("event-" + write.eventId() + "/section-" + write.sectionCode());
      // Range partitioning does not hash: consecutive months share a shard, in ring order.
      case BOOKING_MONTH -> {
        int shardIndex = write.monthIndex() * ring.shards().size() / PLANNED_MONTHS;
        yield ring.shards().get(shardIndex);
      }
    };
  }

  String label() {
    return label;
  }

  /** Whether every competing claim on one seat lands on the same shard (one_claim_per_seat). */
  boolean keepsSeatRuleOnOneShard() {
    return keepsSeatRuleOnOneShard;
  }

  /** Whether all of one customer's holds land on the same shard (the four-holds trigger). */
  boolean keepsHoldLimitOnOneShard() {
    return keepsHoldLimitOnOneShard;
  }

  /** Returns "yes" or "no" for a printed table. */
  static String yesOrNo(boolean answer) {
    return answer ? "yes" : "no";
  }
}
