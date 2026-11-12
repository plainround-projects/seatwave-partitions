package io.plainround.seatwave.partitions.routing;

import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/**
 * The text each kind of row is placed by on the ring. These strings are part of the contract: every
 * router hashes the same text, so changing one moves every row of that kind (Section 8, Step 3).
 * Stateless.
 */
public final class ShardKeys {

  private ShardKeys() {}

  /**
   * Returns the key of a seat's section, which places its seats, holds and bookings.
   *
   * @param eventId the event
   * @param sectionCode the section, A to T for the arena
   * @return {@code event-<id>/section-<code>}, for example {@code event-1/section-A}
   */
  public static String section(long eventId, String sectionCode) {
    return String.format(
        Locale.ROOT, "event-%d/section-%s", eventId, Objects.requireNonNull(sectionCode));
  }

  /**
   * Returns the key of a customer, which places the customer's row, reservations and "my tickets".
   *
   * @param customerId the customer
   * @return {@code customer-<id>}
   */
  public static String customer(long customerId) {
    return "customer-" + customerId;
  }

  /**
   * Returns the key of a hold's or booking's route ({@code id_routes}): the id itself.
   *
   * @param entityId the hold or booking id
   * @return the id's canonical text
   */
  public static String route(UUID entityId) {
    return Objects.requireNonNull(entityId, "entityId").toString();
  }
}
