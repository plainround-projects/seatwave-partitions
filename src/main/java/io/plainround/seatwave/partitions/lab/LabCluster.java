package io.plainround.seatwave.partitions.lab;

import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.partitions.config.Databases;
import io.plainround.seatwave.partitions.config.StackSettings;
import io.plainround.seatwave.partitions.routing.ShardAddress;
import io.plainround.seatwave.partitions.routing.ShardRouter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * The lab's sharded SeatWave on the databases of {@code compose.yaml}: the directory with ring
 * version 1 (64 shards {@code shard-00} to {@code shard-63}, 1,024 points each, LIVE), and the two
 * shard machines with 32 shard schemas each, {@code shard-00} to {@code shard-31} on the first and
 * the rest on the second ("32 to a machine on two machines", Section 10's rollout). It also writes
 * events, sections and seats, and puts everything back between test classes.
 *
 * <p>Not thread-safe: set-up runs on one thread before the work starts. It holds the owner's and
 * the superuser's pools; the caller closes it.
 */
public final class LabCluster implements AutoCloseable {

  /** SeatWave's planned shard count. */
  public static final int SHARD_COUNT = 64;

  /** Points per shard on the ring, from RingSimulationCli's Part 3. */
  public static final int VIRTUAL_NODES_PER_SHARD = 1_024;

  /** The one ring version the lab uses. */
  public static final int RING_VERSION = 1;

  /** Seats per section, Module 13's arena (20 sections of 1,000). */
  public static final int SEATS_PER_SECTION = 1_000;

  private static final String SHARD_TABLES =
      "venues, events, sections, seats, customers, holds, bookings, id_routes,"
          + " hold_reservations, bookings_by_customer, move_markers";

  private final StackSettings settings;
  private final HikariDataSource directoryOwner;
  private final Map<String, HikariDataSource> hostOwners;
  private final Map<String, HikariDataSource> hostSuperusers;

  private LabCluster(
      StackSettings settings,
      HikariDataSource directoryOwner,
      Map<String, HikariDataSource> hostOwners,
      Map<String, HikariDataSource> hostSuperusers) {
    this.settings = settings;
    this.directoryOwner = directoryOwner;
    this.hostOwners = Map.copyOf(hostOwners);
    this.hostSuperusers = Map.copyOf(hostSuperusers);
  }

  /**
   * Migrates the directory and both machines and opens the set-up pools. Safe to run again: every
   * migration and every set-up row is applied only once.
   *
   * @param settings the settings
   * @return the cluster
   * @throws IOException if a password file cannot be read
   * @throws SQLException if a database refuses the set-up
   */
  public static LabCluster prepare(StackSettings settings) throws IOException, SQLException {
    Objects.requireNonNull(settings, "settings");
    Databases.migrate(
        settings,
        settings.directoryUrl(),
        Databases.DIRECTORY_SCHEMA,
        Databases.Migrations.DIRECTORY);
    HikariDataSource directoryOwner =
        Databases.pool(settings, settings.directoryUrl(), Databases.Role.OWNER, "directory");
    Map<String, HikariDataSource> owners = new LinkedHashMap<>();
    Map<String, HikariDataSource> superusers = new LinkedHashMap<>();
    try {
      for (Map.Entry<String, String> host : settings.hostUrls().entrySet()) {
        owners.put(
            host.getKey(),
            Databases.pool(settings, host.getValue(), Databases.Role.OWNER, host.getKey()));
        superusers.put(
            host.getKey(),
            Databases.pool(settings, host.getValue(), Databases.Role.SUPERUSER, host.getKey()));
      }
    } catch (IOException | RuntimeException failure) {
      // Every pool opened so far is closed on the failure path too.
      owners.values().forEach(HikariDataSource::close);
      superusers.values().forEach(HikariDataSource::close);
      directoryOwner.close();
      throw failure;
    }
    LabCluster cluster = new LabCluster(settings, directoryOwner, owners, superusers);
    try {
      cluster.writeRingIfAbsent();
      for (String shardName : shardNames()) {
        cluster.migrateShard(cluster.initialHostOf(shardName), shardName, true);
      }
      return cluster;
    } catch (IOException | SQLException | RuntimeException failure) {
      cluster.close();
      throw failure;
    }
  }

  /** Returns the 64 shard names, {@code shard-00} to {@code shard-63}. */
  public static List<String> shardNames() {
    return IntStream.range(0, SHARD_COUNT)
        .mapToObj(shardNumber -> String.format(Locale.ROOT, "shard-%02d", shardNumber))
        .toList();
  }

  /**
   * Returns the machine that serves a shard before any move.
   *
   * @param shardName a shard name
   * @return the first machine for shards 00 to 31, the second for the rest
   */
  public String initialHostOf(String shardName) {
    int shardNumber = shardNames().indexOf(shardName);
    if (shardNumber < 0) {
      throw new IllegalArgumentException("not one of the 64 shards: " + shardName);
    }
    return settings.hostNames().get(shardNumber < SHARD_COUNT / 2 ? 0 : 1);
  }

  /**
   * Creates (or brings up to date) one shard's schema on one machine and records whether the
   * machine accepts writes for it.
   *
   * @param hostName the machine
   * @param shardName the shard
   * @param acceptsWrites true for the shard's owner, false for a move's target until the switch
   * @throws IOException if the owner's password file cannot be read
   * @throws SQLException if the machine refuses
   */
  public void migrateShard(String hostName, String shardName, boolean acceptsWrites)
      throws IOException, SQLException {
    String url = settings.hostUrls().get(hostName);
    Databases.migrate(settings, url, Databases.ROUTING_SCHEMA, Databases.Migrations.HOST);
    Databases.migrate(settings, url, ShardAddress.schemaOf(shardName), Databases.Migrations.SHARD);
    try (Connection connection = owner(hostName).getConnection();
        PreparedStatement served =
            connection.prepareStatement(
                """
                INSERT INTO routing.served_shards (shard_name, schema_name, accepts_writes)
                VALUES (?, ?, ?) ON CONFLICT (shard_name) DO NOTHING
                """)) {
      served.setString(1, shardName);
      served.setString(2, ShardAddress.schemaOf(shardName));
      served.setBoolean(3, acceptsWrites);
      served.executeUpdate();
    }
  }

  /**
   * Writes the venue, the events, the sections and the seats of some sections, each on the shard
   * its key names: the section and its seats on the shard of {@code event-<id>/section-<code>}, and
   * the venue and the event on each shard that holds one of the event's sections.
   *
   * @param router a router on the live ring
   * @param sections the sections to create, each with {@link #SEATS_PER_SECTION} seats
   * @throws SQLException if a shard refuses
   */
  public void writeSections(ShardRouter router, List<SectionKey> sections) throws SQLException {
    Map<ShardAddress, List<SectionKey>> byShard = new LinkedHashMap<>();
    for (SectionKey section : sections) {
      byShard
          .computeIfAbsent(router.route(section.shardKey()), ignored -> new ArrayList<>())
          .add(section);
    }
    for (Map.Entry<ShardAddress, List<SectionKey>> shard : byShard.entrySet()) {
      try (Connection connection = owner(shard.getKey().hostName()).getConnection()) {
        connection.setSchema(shard.getKey().schemaName());
        writeSections(connection, shard.getValue());
      }
    }
  }

  /**
   * Puts the lab back to its prepared state: no rows in any shard table, every shard on its first
   * machine, no shard-move publication or subscription, no lease.
   *
   * @throws SQLException if a database refuses
   */
  public void reset() throws SQLException {
    for (String hostName : settings.hostNames()) {
      try (Connection connection = superuser(hostName).getConnection();
          Statement statement = connection.createStatement()) {
        // A subscription left by a failed move would keep copying into the schema dropped below.
        // The names come from the catalogs and were made by ShardMover from checked shard names.
        for (String subscription :
            names(statement, "SELECT subname FROM pg_subscription ORDER BY subname")) {
          statement.execute("ALTER SUBSCRIPTION " + subscription + " DISABLE");
          statement.execute("ALTER SUBSCRIPTION " + subscription + " SET (slot_name = NONE)");
          statement.execute("DROP SUBSCRIPTION " + subscription);
        }
        for (String slot :
            names(statement, "SELECT slot_name FROM pg_replication_slots ORDER BY slot_name")) {
          statement.execute("SELECT pg_drop_replication_slot('" + slot + "')");
        }
        for (String publication :
            names(statement, "SELECT pubname FROM pg_publication ORDER BY pubname")) {
          statement.execute("DROP PUBLICATION " + publication);
        }
      }
    }
    for (String shardName : shardNames()) {
      for (String hostName : settings.hostNames()) {
        resetShardOn(hostName, shardName);
      }
    }
    try (Connection connection = directoryOwner.getConnection();
        PreparedStatement placement =
            connection.prepareStatement(
                "UPDATE directory.ring_shards SET host_name = ? WHERE ring_version = ?"
                    + " AND shard_name = ?");
        Statement statement = connection.createStatement()) {
      for (String shardName : shardNames()) {
        placement.setString(1, initialHostOf(shardName));
        placement.setInt(2, RING_VERSION);
        placement.setString(3, shardName);
        placement.addBatch();
      }
      placement.executeBatch();
      statement.execute("DELETE FROM directory.id_worker_leases");
    }
  }

  /**
   * Returns the owner's pool to one machine.
   *
   * @param hostName the machine
   * @return the pool, closed by {@link #close()}
   */
  public HikariDataSource owner(String hostName) {
    return pool(hostOwners, hostName);
  }

  /**
   * Returns the superuser's pool to one machine, for the shard move's publication and subscription.
   *
   * @param hostName the machine
   * @return the pool, closed by {@link #close()}
   */
  public HikariDataSource superuser(String hostName) {
    return pool(hostSuperusers, hostName);
  }

  /** Returns the owner's pool to the directory, for the directory flip. */
  public HikariDataSource directoryOwner() {
    return directoryOwner;
  }

  /** Returns the settings the cluster was prepared with. */
  public StackSettings settings() {
    return settings;
  }

  @Override
  public void close() {
    hostOwners.values().forEach(HikariDataSource::close);
    hostSuperusers.values().forEach(HikariDataSource::close);
    directoryOwner.close();
  }

  private void writeRingIfAbsent() throws SQLException {
    try (Connection connection = directoryOwner.getConnection()) {
      connection.setAutoCommit(false);
      try (PreparedStatement version =
              connection.prepareStatement(
                  """
                  INSERT INTO directory.ring_versions
                    (ring_version, virtual_nodes_per_shard, state, created_at)
                  VALUES (?, ?, 'LIVE', now()) ON CONFLICT (ring_version) DO NOTHING
                  """);
          PreparedStatement shard =
              connection.prepareStatement(
                  """
                  INSERT INTO directory.ring_shards (ring_version, shard_name, host_name)
                  VALUES (?, ?, ?) ON CONFLICT (ring_version, shard_name) DO NOTHING
                  """)) {
        version.setInt(1, RING_VERSION);
        version.setInt(2, VIRTUAL_NODES_PER_SHARD);
        version.executeUpdate();
        for (String shardName : shardNames()) {
          shard.setInt(1, RING_VERSION);
          shard.setString(2, shardName);
          shard.setString(3, initialHostOf(shardName));
          shard.addBatch();
        }
        shard.executeBatch();
        connection.commit();
      } catch (SQLException failure) {
        connection.rollback();
        throw failure;
      }
    }
  }

  private void resetShardOn(String hostName, String shardName) throws SQLException {
    boolean isInitialHost = hostName.equals(initialHostOf(shardName));
    String schema = ShardAddress.schemaOf(shardName);
    try (Connection connection = owner(hostName).getConnection();
        Statement statement = connection.createStatement()) {
      if (!isInitialHost) {
        // A copy a move made on the other machine: gone, with its migration history.
        statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        try (PreparedStatement forget =
            connection.prepareStatement("DELETE FROM routing.served_shards WHERE shard_name = ?")) {
          forget.setString(1, shardName);
          forget.executeUpdate();
        }
        return;
      }
      try (PreparedStatement own =
          connection.prepareStatement(
              "UPDATE routing.served_shards SET accepts_writes = true WHERE shard_name = ?")) {
        own.setString(1, shardName);
        own.executeUpdate();
      }
      connection.setSchema(schema);
      statement.execute("TRUNCATE " + SHARD_TABLES);
    }
  }

  private static void writeSections(Connection connection, List<SectionKey> sections)
      throws SQLException {
    Long[] eventIds = sections.stream().map(SectionKey::eventId).toArray(Long[]::new);
    String[] sectionCodes = sections.stream().map(SectionKey::sectionCode).toArray(String[]::new);
    try (Statement venue = connection.createStatement();
        PreparedStatement events =
            connection.prepareStatement(
                """
                INSERT INTO events (event_id, venue_id, name, starts_at)
                SELECT DISTINCT event_id, 1, 'Arena event ' || event_id,
                       TIMESTAMPTZ '2026-12-08 20:20:51.783+00'
                FROM unnest(?::bigint[]) AS listed(event_id)
                ON CONFLICT (event_id) DO NOTHING
                """);
        PreparedStatement sectionRows =
            connection.prepareStatement(
                """
                INSERT INTO sections (event_id, section_code, price, currency)
                SELECT event_id, section_code, 80.00, 'USD'
                FROM unnest(?::bigint[], ?::varchar[]) AS listed(event_id, section_code)
                """);
        PreparedStatement seats =
            connection.prepareStatement(
                """
                INSERT INTO seats (event_id, seat_label, section_code, seat_number)
                SELECT event_id, section_code || '-' || seat_number, section_code, seat_number
                FROM unnest(?::bigint[], ?::varchar[]) AS listed(event_id, section_code),
                     generate_series(1, ?) AS numbers(seat_number)
                """)) {
      // Module 13's venue 1, "Arena"; every lab event is an arena event at $80.00 a seat.
      venue.execute(
          "INSERT INTO venues (venue_id, name) VALUES (1, 'Arena') ON CONFLICT DO NOTHING");
      events.setArray(1, connection.createArrayOf("bigint", eventIds));
      events.executeUpdate();
      sectionRows.setArray(1, connection.createArrayOf("bigint", eventIds));
      sectionRows.setArray(2, connection.createArrayOf("varchar", sectionCodes));
      sectionRows.executeUpdate();
      seats.setArray(1, connection.createArrayOf("bigint", eventIds));
      seats.setArray(2, connection.createArrayOf("varchar", sectionCodes));
      seats.setInt(3, SEATS_PER_SECTION);
      seats.executeUpdate();
    }
  }

  private static List<String> names(Statement statement, String query) throws SQLException {
    List<String> names = new ArrayList<>();
    try (ResultSet rows = statement.executeQuery(query)) {
      while (rows.next()) {
        names.add(rows.getString(1));
      }
    }
    return names;
  }

  private static HikariDataSource pool(Map<String, HikariDataSource> pools, String hostName) {
    HikariDataSource pool = pools.get(hostName);
    if (pool == null) {
      throw new IllegalArgumentException("the settings list no machine named " + hostName);
    }
    return pool;
  }
}
