package io.plainround.seatwave.partitions.routing;

import java.util.UUID;

/**
 * Why a write through the router was not done: an expected outcome, returned as a value (code
 * guidelines, rule 47). Each case maps to one answer at the API's edge: {@code 409} for the
 * business conflicts, {@code 503} with {@code Retry-After} for the routing ones.
 */
public sealed interface WriteRefusal {

  /**
   * The machine refused a write for a shard it does not own (or no longer owns): the router's ring
   * is stale. The router reloads the directory and the caller retries once.
   *
   * @param address where the stale router sent the write
   */
  record NotOwner(ShardAddress address) implements WriteRefusal {}

  /**
   * The shard was paused for a switch longer than the router waits; nothing was written.
   *
   * @param shardName the paused shard
   */
  record ShardPaused(String shardName) implements WriteRefusal {}

  /**
   * The seat already has an active or confirmed claim ({@code one_claim_per_seat}).
   *
   * @param eventId the event
   * @param seatLabel the seat
   */
  record SeatTaken(long eventId, String seatLabel) implements WriteRefusal {}

  /**
   * The customer already has four unexpired reservations ({@code reservation_limit_per_customer}).
   *
   * @param customerId the customer
   */
  record HoldLimitReached(long customerId) implements WriteRefusal {}

  /**
   * No customer with this id lives on its shard ({@code reservation_by_a_real_customer}).
   *
   * @param customerId the id that was given
   */
  record UnknownCustomer(long customerId) implements WriteRefusal {}

  /**
   * No active hold with this id exists, through its route or on its shard.
   *
   * @param holdId the id that was given
   */
  record UnknownHold(UUID holdId) implements WriteRefusal {}
}
