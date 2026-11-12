package io.plainround.seatwave.partitions.move;

import io.plainround.seatwave.partitions.lab.LabCluster;
import io.plainround.seatwave.partitions.lab.SectionKey;
import io.plainround.seatwave.partitions.routing.ShardAddress;
import io.plainround.seatwave.partitions.routing.ShardRouter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Loads one shard with sold seats before a move, set-based inside the database: the sections whose
 * key the ring places on the shard (events 1, 2, 3 and so on, sections A to T, kept when they land
 * there), their seats, and for the first {@link #BOOKED_SEATS_PER_SECTION} seats of each a
 * confirmed hold and its booking. The other seats stay free for the writer that runs during the
 * move. Stateless.
 */
public final class ShardSeed {

  /** Seats of each section that the seed sells; seats above it are left for the writer. */
  public static final int BOOKED_SEATS_PER_SECTION = 900;

  private static final int ARENA_SECTIONS = 20;

  // One set-based statement each. The customer ids are arbitrary positive numbers: on a shard the
  // hold has no foreign key to customers (V4), and these customers are not the writer's.
  private static final String HOLDS =
      """
      INSERT INTO holds (hold_id, event_id, seat_label, customer_id, state, created_at, expires_at,
                         ended_at)
      SELECT uuidv7(), event_id, seat_label, 1 + (event_id * 1000 + seat_number) % 100000000,
             'CONFIRMED', TIMESTAMPTZ '2026-11-01 10:00:00+00',
             TIMESTAMPTZ '2026-11-01 10:05:00+00', TIMESTAMPTZ '2026-11-01 10:01:00+00'
      FROM seats WHERE seat_number <= ?
      """;
  private static final String BOOKINGS =
      """
      INSERT INTO bookings (booking_id, hold_id, event_id, seat_label, customer_id, price, currency,
                            confirmed_at)
      SELECT uuidv7(), hold_id, event_id, seat_label, customer_id, 80.00, 'USD', ended_at
      FROM holds WHERE state = 'CONFIRMED'
      """;

  private ShardSeed() {}

  /**
   * Finds sections that the live ring places on one shard.
   *
   * @param router the router on the live ring
   * @param shardName the shard
   * @param count how many sections to find
   * @return the first {@code count} sections, in event and section order, on that shard
   */
  public static List<SectionKey> sectionsOn(ShardRouter router, String shardName, int count) {
    Objects.requireNonNull(shardName, "shardName");
    List<SectionKey> found = new ArrayList<>(count);
    for (long eventId = 1; found.size() < count; eventId++) {
      for (int sectionIndex = 0;
          sectionIndex < ARENA_SECTIONS && found.size() < count;
          sectionIndex++) {
        SectionKey section = new SectionKey(eventId, String.valueOf((char) ('A' + sectionIndex)));
        if (router.route(section.shardKey()).shardName().equals(shardName)) {
          found.add(section);
        }
      }
    }
    return List.copyOf(found);
  }

  /**
   * Writes the sections, their seats, and a sold hold and booking for each booked seat.
   *
   * @param cluster the lab
   * @param router the router on the live ring
   * @param sections sections that all live on one shard
   * @return how many holds (and as many bookings) the seed wrote
   * @throws SQLException if the shard refuses
   */
  public static long seed(LabCluster cluster, ShardRouter router, List<SectionKey> sections)
      throws SQLException {
    if (sections.isEmpty()) {
      return 0;
    }
    cluster.writeSections(router, sections);
    ShardAddress shard = router.route(sections.getFirst().shardKey());
    try (Connection connection = cluster.owner(shard.hostName()).getConnection();
        PreparedStatement holds = connection.prepareStatement(HOLDS);
        PreparedStatement bookings = connection.prepareStatement(BOOKINGS)) {
      connection.setSchema(shard.schemaName());
      holds.setInt(1, BOOKED_SEATS_PER_SECTION);
      long written = holds.executeUpdate();
      bookings.executeUpdate();
      return written;
    }
  }
}
