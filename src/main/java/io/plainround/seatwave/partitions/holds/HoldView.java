package io.plainround.seatwave.partitions.holds;

import java.util.Objects;
import java.util.UUID;

/**
 * A hold as {@link HoldFlow#findHold(UUID)} reads it from its seat's shard. Immutable.
 *
 * @param holdId the hold
 * @param eventId the event
 * @param seatLabel the seat
 * @param customerId who holds it
 * @param state ACTIVE, RELEASED, EXPIRED or CONFIRMED
 */
public record HoldView(UUID holdId, long eventId, String seatLabel, long customerId, String state) {

  /** Checks that nothing is missing. */
  public HoldView {
    Objects.requireNonNull(holdId, "holdId");
    Objects.requireNonNull(seatLabel, "seatLabel");
    Objects.requireNonNull(state, "state");
  }
}
