package io.plainround.seatwave.partitions.routing;

import com.github.sviperll.result4j.Result;
import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.partitions.config.Databases;
import io.plainround.seatwave.partitions.config.StackSettings;
import io.plainround.seatwave.partitions.ring.HashRing;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Sends each key's reads and writes to the machine that serves its shard. The ring is built from
 * the directory's LIVE ring version ({@code ring_versions}, {@link HashRing#of}); the machine of
 * each shard comes from {@code ring_shards}. Both are read by {@link #reload()}, so every router
 * that read the same version routes every key the same.
 *
 * <p>Writes go through a per-shard gate: a write holds the gate's shared side while it runs, and
 * {@link #pause(String)} takes the exclusive side, so a shard move can stop one shard's writes for
 * its switch while the other 63 shards keep working. A write that finds its shard paused waits up
 * to {@code router.pauseWaitMillis} and routes again after the pause, so it follows the shard to
 * its new machine. The router does not trust itself: every shard machine refuses a write for a
 * shard it does not own ({@code write_to_owner_only}), and the router turns that refusal into
 * {@link WriteRefusal.NotOwner}.
 *
 * <p>Thread-safe: the live ring is one immutable snapshot behind a volatile reference, replaced
 * whole by {@link #reload()}; the gates are fair read-write locks; the pools are HikariCP's.
 */
public final class ShardRouter implements AutoCloseable {

  private final HikariDataSource directory;
  private final Map<String, HikariDataSource> hostPools;
  private final Duration pauseWait;
  private final Map<String, ReentrantReadWriteLock> gates = new ConcurrentHashMap<>();
  private volatile LiveRing live;

  private ShardRouter(
      HikariDataSource directory, Map<String, HikariDataSource> hostPools, Duration pauseWait)
      throws SQLException {
    this.directory = directory;
    this.hostPools = Map.copyOf(hostPools);
    this.pauseWait = pauseWait;
    this.live = readLiveRing();
  }

  /**
   * Opens a router: one application pool to the directory and one to each shard machine, then reads
   * the live ring.
   *
   * @param settings the settings
   * @param name a name for the pools' log lines, for example {@code router-a}
   * @return the router; the caller closes it
   * @throws IOException if a password file cannot be read
   * @throws SQLException if the directory cannot be read or has no LIVE ring version
   */
  public static ShardRouter open(StackSettings settings, String name)
      throws IOException, SQLException {
    Objects.requireNonNull(settings, "settings");
    Objects.requireNonNull(name, "name");
    HikariDataSource directory =
        Databases.pool(
            settings, settings.directoryUrl(), Databases.Role.APPLICATION, name + "-directory");
    Map<String, HikariDataSource> hostPools = new LinkedHashMap<>();
    try {
      for (Map.Entry<String, String> host : settings.hostUrls().entrySet()) {
        hostPools.put(
            host.getKey(),
            Databases.pool(
                settings, host.getValue(), Databases.Role.APPLICATION, name + "-" + host.getKey()));
      }
      return new ShardRouter(directory, hostPools, settings.pauseWait());
    } catch (IOException | SQLException | RuntimeException failure) {
      // Every pool opened so far is closed on the failure path too.
      hostPools.values().forEach(HikariDataSource::close);
      directory.close();
      throw failure;
    }
  }

  /**
   * Reads the directory again and replaces the ring and the shard-to-machine map in one step. A
   * request already routed keeps the snapshot it used.
   *
   * @throws SQLException if the directory cannot be read or has no LIVE ring version
   */
  public void reload() throws SQLException {
    live = readLiveRing();
  }

  /** Returns the ring version this router routes by. */
  public int ringVersion() {
    return live.ringVersion();
  }

  /**
   * Returns where a key lives on the router's current ring.
   *
   * @param key a shard key, such as {@code event-1/section-A} or {@code customer-42}
   * @return the ring version, shard and machine
   */
  public ShardAddress route(String key) {
    return live.addressOf(Objects.requireNonNull(key, "key"));
  }

  /**
   * Runs one write transaction on the shard of a key.
   *
   * @param key the shard key
   * @param work the transaction's work
   * @param <T> what the work returns
   * @return the work's value, or why nothing was written: the work's own refusal, {@link
   *     WriteRefusal.NotOwner} if the machine does not own the shard, or {@link
   *     WriteRefusal.ShardPaused} if the shard stayed paused longer than the router waits
   * @throws SQLException if the database fails in a way no refusal covers
   * @throws InterruptedException if the thread is interrupted while it waits for a paused shard
   */
  public <T> Result<T, WriteRefusal> write(String key, ShardWork<T> work)
      throws SQLException, InterruptedException {
    Objects.requireNonNull(work, "work");
    String shardName = route(key).shardName();
    Lock shared = gate(shardName).readLock();
    if (!shared.tryLock(pauseWait.toNanos(), TimeUnit.NANOSECONDS)) {
      return Result.error(new WriteRefusal.ShardPaused(shardName));
    }
    try {
      // Routed again under the gate: a write that waited out a switch goes to the new machine.
      ShardAddress address = route(key);
      return runInTransaction(address, work);
    } finally {
      shared.unlock();
    }
  }

  /**
   * Runs one read-only transaction on the shard of a key. Reads take no gate: during a switch both
   * machines hold the same rows, and the old one refuses only writes.
   *
   * @param key the shard key
   * @param work the transaction's work
   * @param <T> what the work returns
   * @return the work's value or refusal
   * @throws SQLException if the database fails
   */
  public <T> Result<T, WriteRefusal> read(String key, ShardWork<T> work) throws SQLException {
    Objects.requireNonNull(work, "work");
    return runInTransaction(route(key), work);
  }

  /**
   * Stops new writes to one shard and waits for the writes in flight to finish. Writes that arrive
   * meanwhile wait (the router's queue) until the returned pause is closed.
   *
   * @param shardName the shard to pause
   * @return the pause; close it, on the same thread, to let writes through again
   * @throws InterruptedException if the thread is interrupted while it waits
   * @throws IllegalStateException if the writes in flight did not finish within the pause wait
   */
  public ShardPause pause(String shardName) throws InterruptedException {
    Lock exclusive = gate(Objects.requireNonNull(shardName, "shardName")).writeLock();
    if (!exclusive.tryLock(pauseWait.toNanos(), TimeUnit.NANOSECONDS)) {
      throw new IllegalStateException(
          "writes to " + shardName + " did not finish within " + pauseWait + "; nothing paused");
    }
    return exclusive::unlock;
  }

  @Override
  public void close() {
    hostPools.values().forEach(HikariDataSource::close);
    directory.close();
  }

  private ReentrantReadWriteLock gate(String shardName) {
    // Fair, so a pause is not starved by a stream of writes, and the writes queued behind a pause
    // go first when it ends.
    return gates.computeIfAbsent(shardName, ignored -> new ReentrantReadWriteLock(true));
  }

  private <T> Result<T, WriteRefusal> runInTransaction(ShardAddress address, ShardWork<T> work)
      throws SQLException {
    HikariDataSource pool = hostPools.get(address.hostName());
    if (pool == null) {
      throw new IllegalStateException(
          "the directory names the machine "
              + address.hostName()
              + " for "
              + address.shardName()
              + ", which the settings do not list");
    }
    try (Connection connection = pool.getConnection()) {
      connection.setSchema(address.schemaName());
      connection.setAutoCommit(false);
      try {
        Result<T, WriteRefusal> result = work.run(connection);
        if (result.discardError().isPresent()) {
          connection.commit();
        } else {
          connection.rollback();
        }
        return result;
      } catch (SQLException failure) {
        connection.rollback();
        if (Constraints.violated(failure)
            .filter(Constraints.WRITE_TO_OWNER_ONLY::equals)
            .isPresent()) {
          return Result.error(new WriteRefusal.NotOwner(address));
        }
        throw failure;
      }
    }
  }

  private LiveRing readLiveRing() throws SQLException {
    try (Connection connection = directory.getConnection()) {
      // One snapshot for both queries, so the version and its shard list always match.
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      try {
        LiveRing ring = readLiveRing(connection);
        connection.commit();
        return ring;
      } catch (SQLException failure) {
        connection.rollback();
        throw failure;
      }
    }
  }

  private static LiveRing readLiveRing(Connection connection) throws SQLException {
    int ringVersion;
    int virtualNodes;
    try (PreparedStatement version =
            connection.prepareStatement(
                "SELECT ring_version, virtual_nodes_per_shard FROM directory.ring_versions"
                    + " WHERE state = 'LIVE'");
        ResultSet row = version.executeQuery()) {
      if (!row.next()) {
        throw new SQLException("the directory has no LIVE ring version");
      }
      ringVersion = row.getInt(1);
      virtualNodes = row.getInt(2);
    }
    Map<String, String> hostByShard = new HashMap<>();
    List<String> shardNames = new ArrayList<>();
    try (PreparedStatement shards =
        connection.prepareStatement(
            "SELECT shard_name, host_name FROM directory.ring_shards WHERE ring_version = ?"
                + " ORDER BY shard_name")) {
      shards.setInt(1, ringVersion);
      try (ResultSet rows = shards.executeQuery()) {
        while (rows.next()) {
          shardNames.add(rows.getString(1));
          hostByShard.put(rows.getString(1), rows.getString(2));
        }
      }
    }
    return new LiveRing(ringVersion, HashRing.of(shardNames, virtualNodes), hostByShard);
  }

  /** What the router routes by: one immutable snapshot of the directory. */
  private record LiveRing(int ringVersion, HashRing ring, Map<String, String> hostByShard) {

    LiveRing {
      hostByShard = Map.copyOf(hostByShard);
    }

    ShardAddress addressOf(String key) {
      String shardName = ring.shardFor(key);
      String hostName =
          Optional.ofNullable(hostByShard.get(shardName))
              .orElseThrow(() -> new IllegalStateException("no machine for " + shardName));
      return new ShardAddress(ringVersion, shardName, hostName);
    }
  }
}
