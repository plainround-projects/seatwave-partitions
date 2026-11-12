package io.plainround.seatwave.partitions.move;

import io.plainround.seatwave.partitions.lab.LabCluster;
import io.plainround.seatwave.partitions.routing.ShardAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * Counts, after a move, whether any acknowledged write was lost or doubled. Immutable.
 *
 * @param seededHolds holds (and bookings) the seed wrote before the move
 * @param ackedHolds holds the writer's flow acknowledged
 * @param ackedBookings bookings the writer's flow acknowledged
 * @param targetHolds holds on the new machine
 * @param targetBookings bookings on the new machine
 * @param ackedHoldsFound acknowledged hold ids found on the new machine
 * @param ackedBookingsFound acknowledged booking ids found on the new machine
 * @param sourceHolds holds left on the old machine (frozen at the fence)
 * @param sourceOnlyHolds writer holds on the old machine that are missing on the new one: a write
 *     the old machine accepted after the fence
 */
public record MoveAudit(
    long seededHolds,
    long ackedHolds,
    long ackedBookings,
    long targetHolds,
    long targetBookings,
    long ackedHoldsFound,
    long ackedBookingsFound,
    long sourceHolds,
    long sourceOnlyHolds) {

  // The seed's holds are dated 2026-11-01 (ShardSeed); the writer's are made at run time.
  private static final String WRITER_HOLDS =
      "SELECT hold_id FROM holds WHERE created_at > TIMESTAMPTZ '2026-11-02 00:00:00+00'";

  /** Returns acknowledged holds and bookings missing on the new machine. */
  public long lost() {
    return (ackedHolds - ackedHoldsFound) + (ackedBookings - ackedBookingsFound);
  }

  /**
   * Returns rows on the new machine beyond the seed and the acknowledged writes, plus writes the
   * old machine took after the fence: each would be a write applied twice or in two places.
   */
  public long doubled() {
    return (targetHolds - seededHolds - ackedHolds)
        + (targetBookings - seededHolds - ackedBookings)
        + sourceOnlyHolds;
  }

  /**
   * Counts both machines' copies of a moved shard.
   *
   * @param cluster the lab
   * @param report the move
   * @param seededHolds what the seed wrote
   * @param tally what the writer did
   * @return the counts
   * @throws SQLException if a machine fails
   */
  public static MoveAudit of(
      LabCluster cluster, MoveReport report, long seededHolds, WriterTally tally)
      throws SQLException {
    Objects.requireNonNull(report, "report");
    Objects.requireNonNull(tally, "tally");
    String schema = ShardAddress.schemaOf(report.shardName());
    DataSource target = cluster.owner(report.toHost());
    DataSource source = cluster.owner(report.fromHost());
    List<UUID> writerHoldsOnSource = ids(source, schema, WRITER_HOLDS);
    long writerHoldsOnBoth = countIn(target, schema, "holds", "hold_id", writerHoldsOnSource);
    return new MoveAudit(
        seededHolds,
        tally.heldIds().size(),
        tally.bookedIds().size(),
        count(target, schema, "SELECT count(*) FROM holds"),
        count(target, schema, "SELECT count(*) FROM bookings"),
        countIn(target, schema, "holds", "hold_id", tally.heldIds()),
        countIn(target, schema, "bookings", "booking_id", tally.bookedIds()),
        count(source, schema, "SELECT count(*) FROM holds"),
        writerHoldsOnSource.size() - writerHoldsOnBoth);
  }

  private static long count(DataSource owner, String schema, String sql) throws SQLException {
    try (Connection connection = owner.getConnection()) {
      connection.setSchema(schema);
      try (PreparedStatement query = connection.prepareStatement(sql);
          ResultSet row = query.executeQuery()) {
        row.next();
        return row.getLong(1);
      }
    }
  }

  // The table and column are this class's own constants, never input.
  private static long countIn(
      DataSource owner, String schema, String table, String column, List<UUID> ids)
      throws SQLException {
    try (Connection connection = owner.getConnection()) {
      connection.setSchema(schema);
      try (PreparedStatement query =
          connection.prepareStatement(
              "SELECT count(DISTINCT "
                  + column
                  + ") FROM "
                  + table
                  + " WHERE "
                  + column
                  + " = ANY (?)")) {
        query.setArray(1, connection.createArrayOf("uuid", ids.toArray()));
        try (ResultSet row = query.executeQuery()) {
          row.next();
          return row.getLong(1);
        }
      }
    }
  }

  private static List<UUID> ids(DataSource owner, String schema, String sql) throws SQLException {
    try (Connection connection = owner.getConnection()) {
      connection.setSchema(schema);
      List<UUID> ids = new ArrayList<>();
      try (PreparedStatement query = connection.prepareStatement(sql);
          ResultSet rows = query.executeQuery()) {
        while (rows.next()) {
          ids.add(rows.getObject(1, UUID.class));
        }
      }
      return ids;
    }
  }
}
