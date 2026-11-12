package io.plainround.seatwave.partitions.holds;

import com.github.sviperll.result4j.Result;
import io.plainround.seatwave.partitions.routing.Constraints;
import io.plainround.seatwave.partitions.routing.ShardKeys;
import io.plainround.seatwave.partitions.routing.ShardRouter;
import io.plainround.seatwave.partitions.routing.WriteRefusal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Holds and confirms seats on the sharded schema, in the fail-safe order of Section 5e. A hold is
 * three transactions on up to three shards: (a) on the customer's shard, a reservation, which the
 * trigger {@code reservation_limit_per_customer} allows only while the customer has fewer than four
 * unexpired ones; (b) on the shard of the new hold id, its route in {@code id_routes}; (c) on the
 * seat's shard, the claim, which {@code one_claim_per_seat} decides. If (c) is refused, the
 * reservation is deleted; if the process dies before that, the reservation expires with the hold,
 * so a failure can block a customer's slot for at most the hold's duration and can never give a
 * fifth hold. Every time is the database's ({@code now()}), so expiry needs no agreement between
 * the clocks of the instances.
 *
 * <p>Thread-safe: stateless apart from the router, which is.
 */
public final class HoldFlow {

  // Module 7's seat label: a section code of capital letters, a hyphen, a seat number.
  private static final Pattern SEAT_LABEL = Pattern.compile("([A-Z]{1,4})-([1-9][0-9]{0,3})");
  private static final int MY_TICKETS_PAGE = 100;

  private static final String RESERVE =
      """
      INSERT INTO hold_reservations (customer_id, hold_id, expires_at)
      VALUES (?, uuidv7(), now() + make_interval(secs => ?))
      RETURNING hold_id
      """;
  private static final String ROUTE =
      "INSERT INTO id_routes (entity_id, event_id, section_code) VALUES (?, ?, ?)";
  // Lazy expiry, as in Module 13: a hold past its time still occupies one_claim_per_seat until the
  // next hold on the seat ends it in the same transaction.
  private static final String EXPIRE_OLD_CLAIM =
      """
      UPDATE holds SET state = 'EXPIRED', ended_at = now()
      WHERE event_id = ? AND seat_label = ? AND state = 'ACTIVE' AND expires_at <= now()
      """;
  private static final String CLAIM =
      """
      INSERT INTO holds (hold_id, event_id, seat_label, customer_id, state, created_at, expires_at)
      VALUES (?, ?, ?, ?, 'ACTIVE', now(), now() + make_interval(secs => ?))
      """;
  private static final String UNRESERVE =
      "DELETE FROM hold_reservations WHERE customer_id = ? AND hold_id = ?";
  private static final String FIND_ROUTE =
      "SELECT event_id, section_code FROM id_routes WHERE entity_id = ?";
  private static final String CONFIRM_HOLD =
      """
      UPDATE holds SET state = 'CONFIRMED', ended_at = now()
      WHERE hold_id = ? AND state = 'ACTIVE' AND expires_at > now()
      RETURNING event_id, seat_label, customer_id
      """;
  private static final String BOOK =
      """
      INSERT INTO bookings (hold_id, event_id, seat_label, customer_id, price, currency, confirmed_at)
      SELECT ?, event_id, ?, ?, price, currency, now() FROM sections
      WHERE event_id = ? AND section_code = ?
      RETURNING booking_id, confirmed_at
      """;
  private static final String LIST_FOR_CUSTOMER =
      """
      INSERT INTO bookings_by_customer (customer_id, confirmed_at, booking_id, event_id, section_code)
      VALUES (?, ?, ?, ?, ?)
      """;
  private static final String FIND_HOLD =
      "SELECT event_id, seat_label, customer_id, state FROM holds WHERE hold_id = ?";
  private static final String MY_TICKETS =
      """
      SELECT booking_id, event_id, section_code, confirmed_at FROM bookings_by_customer
      WHERE customer_id = ? ORDER BY confirmed_at DESC, booking_id LIMIT ?
      """;

  private final ShardRouter router;
  private final Duration holdDuration;

  /**
   * Creates the flow.
   *
   * @param router the router every step goes through
   * @param holdDuration how long a hold and its reservation last (Module 1: 300 s)
   */
  public HoldFlow(ShardRouter router, Duration holdDuration) {
    this.router = Objects.requireNonNull(router, "router");
    this.holdDuration = Objects.requireNonNull(holdDuration, "holdDuration");
  }

  /**
   * Holds one seat for a customer.
   *
   * @param customerId the customer, whose row lives on the shard of {@code customer-<id>}
   * @param eventId the event
   * @param seatLabel the seat, for example {@code A-7}
   * @return the new hold's id (a version 7 UUID made by the customer's shard), or why there is no
   *     hold: {@link WriteRefusal.HoldLimitReached}, {@link WriteRefusal.UnknownCustomer}, {@link
   *     WriteRefusal.SeatTaken}, or a routing refusal of any of the three steps
   * @throws SQLException if a database fails in a way no refusal covers
   * @throws InterruptedException if the thread is interrupted while a shard is paused
   * @throws IllegalArgumentException if the seat label is malformed
   */
  public Result<UUID, WriteRefusal> hold(long customerId, long eventId, String seatLabel)
      throws SQLException, InterruptedException {
    String sectionCode = sectionOf(seatLabel);
    Result<UUID, WriteRefusal> reserved =
        router.write(ShardKeys.customer(customerId), connection -> reserve(connection, customerId));
    if (!(reserved instanceof Result.Success<UUID, WriteRefusal>(UUID holdId))) {
      return reserved;
    }
    Result<UUID, WriteRefusal> claimed =
        switch (router.write(
            ShardKeys.route(holdId),
            connection -> route(connection, holdId, eventId, sectionCode))) {
          case Result.Error<Boolean, WriteRefusal>(WriteRefusal refusal) -> Result.error(refusal);
          case Result.Success<Boolean, WriteRefusal> _ ->
              router.write(
                  ShardKeys.section(eventId, sectionCode),
                  connection ->
                      claim(
                          connection,
                          new Claim(holdId, customerId, eventId, seatLabel, sectionCode)));
        };
    if (claimed.isError()) {
      // Step (c) of Section 5e. If this compensation itself is refused, the reservation stays
      // until it expires: the customer's slot is blocked for at most the hold's duration, which is
      // the safe direction, so the claim's refusal is the one returned.
      router.write(
          ShardKeys.customer(customerId), connection -> unreserve(connection, customerId, holdId));
    }
    return claimed;
  }

  /**
   * Confirms an active hold: the booking on the seat's shard, then its route and the customer's "my
   * tickets" row, and the reservation is removed because the hold has ended.
   *
   * @param holdId the hold
   * @return the new booking's id (a version 7 UUID), or {@link WriteRefusal.UnknownHold} if no
   *     active, unexpired hold has this id, or a routing refusal
   * @throws SQLException if a database fails in a way no refusal covers
   * @throws InterruptedException if the thread is interrupted while a shard is paused
   */
  public Result<UUID, WriteRefusal> confirm(UUID holdId) throws SQLException, InterruptedException {
    Objects.requireNonNull(holdId, "holdId");
    if (!(findRoute(holdId) instanceof Result.Success<Route, WriteRefusal>(Route seatShard))) {
      return Result.error(new WriteRefusal.UnknownHold(holdId));
    }
    Result<Booking, WriteRefusal> booked =
        router.write(
            ShardKeys.section(seatShard.eventId(), seatShard.sectionCode()),
            connection -> book(connection, holdId, seatShard));
    if (!(booked instanceof Result.Success<Booking, WriteRefusal>(Booking booking))) {
      return booked.map(Booking::bookingId);
    }
    // After the booking committed: the route and the listing are a global index that lags the
    // booking (Section 5e); Module 28 makes this hand-off reliable. A refusal here is returned, so
    // the caller retries the listing, never the booking.
    Result<Boolean, WriteRefusal> routed =
        router.write(
            ShardKeys.route(booking.bookingId()),
            connection ->
                route(
                    connection, booking.bookingId(), seatShard.eventId(), seatShard.sectionCode()));
    if (routed instanceof Result.Error<Boolean, WriteRefusal>(WriteRefusal refusal)) {
      return Result.error(refusal);
    }
    return router.write(
        ShardKeys.customer(booking.customerId()),
        connection -> listForCustomer(connection, booking, seatShard));
  }

  /**
   * Finds a hold by its id alone: its route on the shard of the id, then the hold on its seat's
   * shard (two lookups, Section 5e).
   *
   * @param holdId the hold
   * @return the hold, or {@link WriteRefusal.UnknownHold}
   * @throws SQLException if a database fails
   */
  public Result<HoldView, WriteRefusal> findHold(UUID holdId) throws SQLException {
    Objects.requireNonNull(holdId, "holdId");
    return switch (findRoute(holdId)) {
      case Result.Error<Route, WriteRefusal>(WriteRefusal refusal) -> Result.error(refusal);
      case Result.Success<Route, WriteRefusal>(Route seatShard) ->
          router.read(
              ShardKeys.section(seatShard.eventId(), seatShard.sectionCode()),
              connection -> readHold(connection, holdId));
    };
  }

  /**
   * Lists a customer's newest bookings from {@code bookings_by_customer} on the customer's shard:
   * one shard query instead of a scatter to all 64.
   *
   * @param customerId the customer
   * @return at most 100 booking ids, newest first
   * @throws SQLException if the database fails
   */
  public List<UUID> myTickets(long customerId) throws SQLException {
    return router
        .read(ShardKeys.customer(customerId), connection -> listTickets(connection, customerId))
        .discardError()
        .orElseThrow();
  }

  private Result<Route, WriteRefusal> findRoute(UUID holdId) throws SQLException {
    return router.read(ShardKeys.route(holdId), connection -> readRoute(connection, holdId));
  }

  private Result<UUID, WriteRefusal> reserve(Connection connection, long customerId)
      throws SQLException {
    try (PreparedStatement reserve = connection.prepareStatement(RESERVE)) {
      reserve.setLong(1, customerId);
      reserve.setLong(2, holdDuration.toSeconds());
      try (ResultSet row = reserve.executeQuery()) {
        row.next();
        return Result.success(row.getObject(1, UUID.class));
      }
    } catch (SQLException refused) {
      Optional<String> constraint = Constraints.violated(refused);
      if (constraint.filter("reservation_limit_per_customer"::equals).isPresent()) {
        return Result.error(new WriteRefusal.HoldLimitReached(customerId));
      }
      if (constraint.filter("reservation_by_a_real_customer"::equals).isPresent()) {
        return Result.error(new WriteRefusal.UnknownCustomer(customerId));
      }
      throw refused;
    }
  }

  private static Result<Boolean, WriteRefusal> route(
      Connection connection, UUID entityId, long eventId, String sectionCode) throws SQLException {
    try (PreparedStatement route = connection.prepareStatement(ROUTE)) {
      route.setObject(1, entityId);
      route.setLong(2, eventId);
      route.setString(3, sectionCode);
      route.executeUpdate();
      return Result.success(Boolean.TRUE);
    }
  }

  private Result<UUID, WriteRefusal> claim(Connection connection, Claim claim) throws SQLException {
    try (PreparedStatement expire = connection.prepareStatement(EXPIRE_OLD_CLAIM);
        PreparedStatement insert = connection.prepareStatement(CLAIM)) {
      expire.setLong(1, claim.eventId());
      expire.setString(2, claim.seatLabel());
      expire.executeUpdate();
      insert.setObject(1, claim.holdId());
      insert.setLong(2, claim.eventId());
      insert.setString(3, claim.seatLabel());
      insert.setLong(4, claim.customerId());
      insert.setLong(5, holdDuration.toSeconds());
      insert.executeUpdate();
      return Result.success(claim.holdId());
    } catch (SQLException refused) {
      if (Constraints.violated(refused).filter("one_claim_per_seat"::equals).isPresent()) {
        return Result.error(new WriteRefusal.SeatTaken(claim.eventId(), claim.seatLabel()));
      }
      throw refused;
    }
  }

  private static Result<Boolean, WriteRefusal> unreserve(
      Connection connection, long customerId, UUID holdId) throws SQLException {
    try (PreparedStatement delete = connection.prepareStatement(UNRESERVE)) {
      delete.setLong(1, customerId);
      delete.setObject(2, holdId);
      return Result.success(delete.executeUpdate() == 1);
    }
  }

  private static Result<Route, WriteRefusal> readRoute(Connection connection, UUID holdId)
      throws SQLException {
    try (PreparedStatement query = connection.prepareStatement(FIND_ROUTE)) {
      query.setObject(1, holdId);
      try (ResultSet row = query.executeQuery()) {
        if (!row.next()) {
          return Result.error(new WriteRefusal.UnknownHold(holdId));
        }
        return Result.success(new Route(row.getLong(1), row.getString(2)));
      }
    }
  }

  private static Result<Booking, WriteRefusal> book(
      Connection connection, UUID holdId, Route seatShard) throws SQLException {
    long customerId;
    String seatLabel;
    try (PreparedStatement confirm = connection.prepareStatement(CONFIRM_HOLD)) {
      confirm.setObject(1, holdId);
      try (ResultSet row = confirm.executeQuery()) {
        if (!row.next()) {
          return Result.error(new WriteRefusal.UnknownHold(holdId));
        }
        seatLabel = row.getString(2);
        customerId = row.getLong(3);
      }
    }
    try (PreparedStatement book = connection.prepareStatement(BOOK)) {
      book.setObject(1, holdId);
      book.setString(2, seatLabel);
      book.setLong(3, customerId);
      book.setLong(4, seatShard.eventId());
      book.setString(5, seatShard.sectionCode());
      try (ResultSet row = book.executeQuery()) {
        row.next();
        return Result.success(
            new Booking(
                row.getObject(1, UUID.class),
                holdId,
                customerId,
                row.getObject(2, OffsetDateTime.class).toInstant()));
      }
    }
  }

  private static Result<UUID, WriteRefusal> listForCustomer(
      Connection connection, Booking booking, Route seatShard) throws SQLException {
    try (PreparedStatement list = connection.prepareStatement(LIST_FOR_CUSTOMER);
        PreparedStatement unreserve = connection.prepareStatement(UNRESERVE)) {
      list.setLong(1, booking.customerId());
      list.setObject(2, booking.confirmedAt().atOffset(ZoneOffset.UTC));
      list.setObject(3, booking.bookingId());
      list.setLong(4, seatShard.eventId());
      list.setString(5, seatShard.sectionCode());
      list.executeUpdate();
      // The hold has ended, so it no longer counts against the four.
      unreserve.setLong(1, booking.customerId());
      unreserve.setObject(2, booking.holdId());
      unreserve.executeUpdate();
      return Result.success(booking.bookingId());
    }
  }

  private static Result<HoldView, WriteRefusal> readHold(Connection connection, UUID holdId)
      throws SQLException {
    try (PreparedStatement query = connection.prepareStatement(FIND_HOLD)) {
      query.setObject(1, holdId);
      try (ResultSet row = query.executeQuery()) {
        if (!row.next()) {
          return Result.error(new WriteRefusal.UnknownHold(holdId));
        }
        return Result.success(
            new HoldView(
                holdId, row.getLong(1), row.getString(2), row.getLong(3), row.getString(4)));
      }
    }
  }

  private static Result<List<UUID>, WriteRefusal> listTickets(
      Connection connection, long customerId) throws SQLException {
    try (PreparedStatement query = connection.prepareStatement(MY_TICKETS)) {
      query.setLong(1, customerId);
      query.setInt(2, MY_TICKETS_PAGE);
      List<UUID> bookingIds = new ArrayList<>();
      try (ResultSet rows = query.executeQuery()) {
        while (rows.next()) {
          bookingIds.add(rows.getObject(1, UUID.class));
        }
      }
      return Result.success(List.copyOf(bookingIds));
    }
  }

  private static String sectionOf(String seatLabel) {
    Matcher label = SEAT_LABEL.matcher(Objects.requireNonNull(seatLabel, "seatLabel"));
    if (!label.matches()) {
      throw new IllegalArgumentException(
          "a seat label is a section code, a hyphen and a number, such as A-7: " + seatLabel);
    }
    return label.group(1);
  }

  /** What the claim step writes. */
  private record Claim(
      UUID holdId, long customerId, long eventId, String seatLabel, String sectionCode) {}

  /** A route row: the shard key of a hold or booking. */
  private record Route(long eventId, String sectionCode) {}

  /** A booking just written, for the steps after it. */
  private record Booking(UUID bookingId, UUID holdId, long customerId, Instant confirmedAt) {}
}
