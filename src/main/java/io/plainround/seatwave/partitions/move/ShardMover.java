package io.plainround.seatwave.partitions.move;

import io.plainround.seatwave.partitions.config.Databases;
import io.plainround.seatwave.partitions.config.StackSettings;
import io.plainround.seatwave.partitions.lab.LabCluster;
import io.plainround.seatwave.partitions.routing.ShardAddress;
import io.plainround.seatwave.partitions.routing.ShardPause;
import io.plainround.seatwave.partitions.routing.ShardRouter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Moves one whole shard from one machine to another while it takes writes, in the steps of Section
 * 6e: (1) copy, by PostgreSQL logical replication (a publication of the shard's tables on the old
 * machine, a subscription on the new one, which copies the rows and then streams every change); (2)
 * catch up; (4) switch writes; (5) clean up. Step (3), dual reads, is not built: the router reads
 * from the owner only, and both machines hold the same rows at the switch.
 *
 * <p>The switch keeps the move's one rule, that every key has exactly one place that accepts its
 * writes: the router pauses the shard; the fence on the old machine locks the shard's tables in
 * SHARE mode (it waits for every write in flight, from this router or a stale one), turns off its
 * {@code accepts_writes} and writes a marker row in the same transaction; the move waits until the
 * marker is visible on the new machine, which proves every earlier write arrived (logical
 * replication applies in commit order); then the directory names the new machine, the new machine
 * accepts writes, the router reloads, and the pause ends. A write that reaches the old machine
 * after the fence, from a router that has not reloaded, is refused there ({@code
 * write_to_owner_only}).
 *
 * <p>Not thread-safe: one move at a time, run on one thread (the pause belongs to that thread).
 */
public final class ShardMover {

  // The tables of a shard, in the order the fence locks them: the order the hold flow's
  // transactions take them (holds before bookings, bookings_by_customer before hold_reservations),
  // so a write in flight never waits for a table the fence holds while the fence waits for it.
  static final List<String> SHARD_TABLES =
      List.of(
          "venues",
          "events",
          "sections",
          "seats",
          "customers",
          "holds",
          "bookings",
          "id_routes",
          "bookings_by_customer",
          "hold_reservations",
          "move_markers");

  // The generated password is hex (scripts/stack.sh); it is checked before it goes into the
  // subscription's connection string, which DDL cannot take as a parameter.
  private static final Pattern HEX_PASSWORD = Pattern.compile("[0-9a-f]{16,128}");
  private static final long POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
  private static final long COPY_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
  // The Compose service name is the machine's name on the project's network, on PostgreSQL's
  // own port; the subscriber connects from inside that network, not through the published port.
  private static final int CONTAINER_PORT = 5432;

  private final LabCluster cluster;
  private final ShardRouter router;
  private final StackSettings settings;

  /**
   * Creates a mover.
   *
   * @param cluster the lab, for the owner's and the superuser's pools
   * @param router the router whose writes the switch pauses and which reloads after it
   */
  public ShardMover(LabCluster cluster, ShardRouter router) {
    this.cluster = Objects.requireNonNull(cluster, "cluster");
    this.router = Objects.requireNonNull(router, "router");
    this.settings = cluster.settings();
  }

  /**
   * Moves a shard and reports how long each step took.
   *
   * @param shardName the shard, for example {@code shard-17}
   * @param fromHost the machine that serves it now
   * @param toHost the machine that serves it afterwards
   * @return the sizes and the timings
   * @throws IOException if a password file cannot be read
   * @throws SQLException if a database refuses a step; nothing has switched unless the report came
   * @throws InterruptedException if the thread is interrupted while it waits
   * @throws IllegalStateException if the copy or the catch-up does not finish in time
   */
  public MoveReport move(String shardName, String fromHost, String toHost)
      throws IOException, SQLException, InterruptedException {
    return finish(copy(shardName, fromHost, toHost));
  }

  /**
   * Steps (1) and (2) of a move: copies the shard to the new machine and waits until the copy is
   * ready and streaming. The old machine still owns the shard and takes its writes.
   *
   * @param shardName the shard, for example {@code shard-17}
   * @param fromHost the machine that serves it now
   * @param toHost the machine that serves it afterwards
   * @return the copy in progress, for {@link #finish(CopyInProgress)}
   * @throws IOException if a password file cannot be read
   * @throws SQLException if a database refuses a step
   * @throws InterruptedException if the thread is interrupted while it waits
   * @throws IllegalStateException if the copy does not finish in time
   */
  public CopyInProgress copy(String shardName, String fromHost, String toHost)
      throws IOException, SQLException, InterruptedException {
    Objects.requireNonNull(shardName, "shardName");
    Objects.requireNonNull(fromHost, "fromHost");
    Objects.requireNonNull(toHost, "toHost");
    String schema = ShardAddress.schemaOf(shardName);
    String moveName = "move_" + schema;
    long[] sourceBytes = sizes(cluster.owner(fromHost), schema);
    cluster.migrateShard(toHost, shardName, false);
    createPublication(fromHost, moveName, schema);
    long copyStarted = System.nanoTime();
    createSubscription(toHost, fromHost, moveName);
    waitForCopy(toHost, moveName);
    return new CopyInProgress(
        shardName,
        fromHost,
        toHost,
        sourceBytes[0],
        sourceBytes[1],
        Duration.ofNanos(System.nanoTime() - copyStarted));
  }

  /**
   * Steps (2) to (5): waits for the copy to catch up, switches writes, and drops the publication
   * and the subscription.
   *
   * @param copy the ready copy
   * @return the sizes and the timings of the whole move
   * @throws SQLException if a database refuses a step
   * @throws InterruptedException if the thread is interrupted while it waits
   * @throws IllegalStateException if the catch-up or the switch does not finish in time
   */
  public MoveReport finish(CopyInProgress copy) throws SQLException, InterruptedException {
    Objects.requireNonNull(copy, "copy");
    String schema = ShardAddress.schemaOf(copy.shardName());
    long catchUpStarted = System.nanoTime();
    waitForMarker(copy.toHost(), schema, writeMarker(copy.fromHost(), schema));
    Duration catchUp = Duration.ofNanos(System.nanoTime() - catchUpStarted);
    Switch switched = switchWrites(copy.shardName(), copy.fromHost(), copy.toHost());
    dropMove(copy.toHost(), copy.fromHost(), "move_" + schema);
    long[] targetBytes = sizes(cluster.owner(copy.toHost()), schema);
    return new MoveReport(
        copy.shardName(),
        copy.fromHost(),
        copy.toHost(),
        router.ringVersion(),
        copy.sourceTableBytes(),
        copy.sourceTotalBytes(),
        targetBytes[1],
        copy.copy(),
        catchUp,
        switched.pause(),
        switched.fence(),
        switched.lagZero());
  }

  private Switch switchWrites(String shardName, String fromHost, String toHost)
      throws SQLException, InterruptedException {
    String schema = ShardAddress.schemaOf(shardName);
    long pauseStarted = System.nanoTime();
    long fenced;
    long arrived;
    try (ShardPause _ = router.pause(shardName)) {
      UUID marker = fence(fromHost, shardName);
      fenced = System.nanoTime();
      waitForMarker(toHost, schema, marker);
      arrived = System.nanoTime();
      try (Connection directory = cluster.directoryOwner().getConnection();
          PreparedStatement flip =
              directory.prepareStatement(
                  "UPDATE directory.ring_shards SET host_name = ? WHERE ring_version = ?"
                      + " AND shard_name = ?")) {
        flip.setString(1, toHost);
        flip.setInt(2, router.ringVersion());
        flip.setString(3, shardName);
        if (flip.executeUpdate() != 1) {
          throw new SQLException("the directory has no row for " + shardName);
        }
      }
      setAcceptsWrites(toHost, shardName, true);
      router.reload();
    }
    long resumed = System.nanoTime();
    return new Switch(
        Duration.ofNanos(resumed - pauseStarted),
        Duration.ofNanos(fenced - pauseStarted),
        Duration.ofNanos(arrived - fenced));
  }

  /** The fence: one transaction on the old machine. Returns the marker it wrote. */
  private UUID fence(String hostName, String shardName) throws SQLException {
    String schema = ShardAddress.schemaOf(shardName);
    UUID marker = UUID.randomUUID();
    try (Connection connection = cluster.owner(hostName).getConnection()) {
      connection.setAutoCommit(false);
      try (PreparedStatement lockTimeout =
              connection.prepareStatement("SELECT set_config('lock_timeout', ?, true)");
          Statement lock = connection.createStatement();
          PreparedStatement stop =
              connection.prepareStatement(
                  "UPDATE routing.served_shards SET accepts_writes = false WHERE shard_name = ?");
          PreparedStatement write =
              connection.prepareStatement(
                  "INSERT INTO "
                      + schema
                      + ".move_markers (marker_id, written_at) VALUES (?, now())")) {
        lockTimeout.setString(1, settings.lagTimeout().toMillis() + "ms");
        lockTimeout.execute();
        // SHARE conflicts with the ROW EXCLUSIVE lock every INSERT, UPDATE and DELETE takes: this
        // waits for the writes in flight and holds new ones until the commit below, after which
        // their ownership check sees accepts_writes = false. The schema name was checked by
        // ShardAddress, and LOCK cannot take table names as parameters.
        lock.execute("LOCK TABLE " + qualified(schema) + " IN SHARE MODE");
        stop.setString(1, shardName);
        if (stop.executeUpdate() != 1) {
          throw new SQLException(hostName + " does not serve " + shardName);
        }
        write.setObject(1, marker);
        write.executeUpdate();
        connection.commit();
        return marker;
      } catch (SQLException failure) {
        connection.rollback();
        throw failure;
      }
    }
  }

  private UUID writeMarker(String hostName, String schema) throws SQLException {
    UUID marker = UUID.randomUUID();
    try (Connection connection = cluster.owner(hostName).getConnection();
        PreparedStatement write =
            connection.prepareStatement(
                "INSERT INTO "
                    + schema
                    + ".move_markers (marker_id, written_at) VALUES (?, now())")) {
      write.setObject(1, marker);
      write.executeUpdate();
    }
    return marker;
  }

  private void waitForMarker(String hostName, String schema, UUID marker)
      throws SQLException, InterruptedException {
    long deadline = System.nanoTime() + settings.lagTimeout().toNanos();
    try (Connection connection = cluster.owner(hostName).getConnection();
        PreparedStatement seen =
            connection.prepareStatement(
                "SELECT count(*) FROM " + schema + ".move_markers WHERE marker_id = ?")) {
      seen.setObject(1, marker);
      while (true) {
        try (ResultSet row = seen.executeQuery()) {
          row.next();
          if (row.getLong(1) == 1) {
            return;
          }
        }
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException(
              "the marker did not reach " + hostName + " within " + settings.lagTimeout());
        }
        sleepNanos(POLL_NANOS);
      }
    }
  }

  private void createPublication(String hostName, String moveName, String schema)
      throws SQLException {
    try (Connection connection = cluster.superuser(hostName).getConnection();
        Statement statement = connection.createStatement()) {
      // A publication of named tables needs their owner or a superuser; the move is an operator's
      // job, so the lab runs it as the superuser (production: a role made for moves).
      statement.execute("CREATE PUBLICATION " + moveName + " FOR TABLE " + qualified(schema));
    }
  }

  private void createSubscription(String hostName, String sourceHost, String moveName)
      throws IOException, SQLException {
    String password = Databases.password(settings.replicatorPasswordFile());
    if (!HEX_PASSWORD.matcher(password).matches()) {
      throw new IOException(
          "the replicator's password file must hold the hex password scripts/stack.sh makes");
    }
    String connection =
        String.format(
            Locale.ROOT,
            "host=%s port=%d dbname=seatwave user=%s password=%s",
            sourceHost,
            CONTAINER_PORT,
            settings.replicatorUser(),
            password);
    try (Connection target = cluster.superuser(hostName).getConnection();
        Statement statement = target.createStatement()) {
      // Creating the subscription creates the slot on the old machine and starts the copy.
      statement.execute(
          "CREATE SUBSCRIPTION "
              + moveName
              + " CONNECTION '"
              + connection
              + "' PUBLICATION "
              + moveName);
    }
  }

  private void waitForCopy(String hostName, String moveName)
      throws SQLException, InterruptedException {
    long deadline = System.nanoTime() + settings.copyTimeout().toNanos();
    try (Connection connection = cluster.superuser(hostName).getConnection();
        PreparedStatement states =
            connection.prepareStatement(
                """
                SELECT count(*) FILTER (WHERE rel.srsubstate = 'r'), count(*)
                FROM pg_subscription_rel AS rel
                JOIN pg_subscription AS sub ON sub.oid = rel.srsubid
                WHERE sub.subname = ?
                """)) {
      states.setString(1, moveName);
      while (true) {
        try (ResultSet row = states.executeQuery()) {
          row.next();
          // Every table is "r" (ready): its copy is done and its changes stream.
          if (row.getLong(2) == SHARD_TABLES.size() && row.getLong(1) == row.getLong(2)) {
            return;
          }
        }
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException(
              "the copy to " + hostName + " did not finish within " + settings.copyTimeout());
        }
        sleepNanos(COPY_POLL_NANOS);
      }
    }
  }

  private void dropMove(String targetHost, String sourceHost, String moveName) throws SQLException {
    // Dropping the subscription also drops its slot on the old machine. The old copy stays there,
    // refusing writes: the rollback path of step (5) until it is dropped.
    try (Connection target = cluster.superuser(targetHost).getConnection();
        Statement statement = target.createStatement()) {
      statement.execute("DROP SUBSCRIPTION " + moveName);
    }
    try (Connection source = cluster.superuser(sourceHost).getConnection();
        Statement statement = source.createStatement()) {
      statement.execute("DROP PUBLICATION " + moveName);
    }
  }

  private void setAcceptsWrites(String hostName, String shardName, boolean acceptsWrites)
      throws SQLException {
    try (Connection connection = cluster.owner(hostName).getConnection();
        PreparedStatement update =
            connection.prepareStatement(
                "UPDATE routing.served_shards SET accepts_writes = ? WHERE shard_name = ?")) {
      update.setBoolean(1, acceptsWrites);
      update.setString(2, shardName);
      if (update.executeUpdate() != 1) {
        throw new SQLException(hostName + " has no served_shards row for " + shardName);
      }
    }
  }

  /** Returns the heap size and the total size (with indexes) of a shard's tables, in bytes. */
  static long[] sizes(DataSource owner, String schema) throws SQLException {
    try (Connection connection = owner.getConnection();
        PreparedStatement query =
            connection.prepareStatement(
                """
                SELECT coalesce(sum(pg_table_size(rel.oid)), 0),
                       coalesce(sum(pg_total_relation_size(rel.oid)), 0)
                FROM pg_class AS rel JOIN pg_namespace AS ns ON ns.oid = rel.relnamespace
                WHERE ns.nspname = ? AND rel.relkind = 'r' AND rel.relname = ANY (?)
                """)) {
      query.setString(1, schema);
      query.setArray(2, connection.createArrayOf("text", SHARD_TABLES.toArray()));
      try (ResultSet row = query.executeQuery()) {
        row.next();
        return new long[] {row.getLong(1), row.getLong(2)};
      }
    }
  }

  private static String qualified(String schema) {
    return String.join(", ", SHARD_TABLES.stream().map(table -> schema + "." + table).toList());
  }

  private static void sleepNanos(long nanos) throws InterruptedException {
    LockSupport.parkNanos(nanos);
    if (Thread.interrupted()) {
      throw new InterruptedException("interrupted while waiting for the shard move");
    }
  }

  /** The switch's three timings. */
  private record Switch(Duration pause, Duration fence, Duration lagZero) {}
}
