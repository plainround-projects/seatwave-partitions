package io.plainround.seatwave.partitions.shardkey;

import java.util.Objects;

/**
 * One booking write as the shard-key simulation sees it: the columns a candidate shard key could be
 * built from. Immutable.
 *
 * @param eventId the event, as in Module 13's {@code events}
 * @param sectionCode the seat's section, the part of the seat label before the hyphen
 * @param customerId the customer, as in Module 13's {@code customers}
 * @param bookingId a random identifier standing in for the booking's id
 * @param monthIndex the month of the booking, counted from the first month of the plan
 */
record BookingWrite(
    long eventId, String sectionCode, long customerId, long bookingId, int monthIndex) {

  BookingWrite {
    if (eventId < 1 || customerId < 1 || monthIndex < 0) {
      throw new IllegalArgumentException("ids start at 1 and months at 0");
    }
    Objects.requireNonNull(sectionCode, "sectionCode");
    if (!sectionCode.matches("[A-Z]{1,4}")) {
      throw new IllegalArgumentException("section code must match Module 13's [A-Z]{1,4}");
    }
  }
}
