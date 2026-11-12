package io.plainround.seatwave.partitions.stack;

import com.github.sviperll.result4j.Result;
import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.partitions.config.Databases;
import io.plainround.seatwave.partitions.config.SettingsReader;
import io.plainround.seatwave.partitions.config.StackSettings;
import io.plainround.seatwave.partitions.holds.Customers;
import io.plainround.seatwave.partitions.ids.IdLayout;
import io.plainround.seatwave.partitions.ids.LeasedIdGenerator;
import io.plainround.seatwave.partitions.ids.WorkerLease;
import io.plainround.seatwave.partitions.ids.WorkerLeases;
import io.plainround.seatwave.partitions.lab.LabCluster;
import io.plainround.seatwave.partitions.routing.ShardAddress;
import io.plainround.seatwave.partitions.routing.ShardRouter;
import io.plainround.seatwave.partitions.routing.WriteRefusal;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * The stack tests' view of the lab: the prepared and reset cluster, one router on the live ring,
 * and an application pool to the directory. One instance per test class, closed after the class.
 * Not thread-safe; the racers use only the router and the pools, which are.
 */
final class StackLab implements AutoCloseable {

  static final Duration BACKWARD_TOLERANCE = Duration.ofMillis(10);

  final StackSettings settings;
  final LabCluster cluster;
  final ShardRouter router;
  final HikariDataSource directory;

  private StackLab(StackSettings settings) throws IOException, SQLException {
    this.settings = settings;
    this.cluster = LabCluster.prepare(settings);
    cluster.reset();
    this.router = ShardRouter.open(settings, "test-router");
    this.directory =
        Databases.pool(settings, settings.directoryUrl(), Databases.Role.APPLICATION, "test");
  }

  /** Opens the lab named by the system property {@code seatwave.stackSettings}, emptied. */
  static StackLab open() throws IOException, SQLException {
    Path file = Path.of(System.getProperty("seatwave.stackSettings", "config/stack.properties"));
    return switch (StackSettings.read(new SettingsReader(SettingsReader.load(file)))) {
      case Result.Success<StackSettings, List<String>>(StackSettings settings) ->
          new StackLab(settings);
      case Result.Error<StackSettings, List<String>>(List<String> problems) ->
          throw new IllegalStateException(
              "the settings in " + file + " have problems: " + problems);
    };
  }

  WorkerLeases leases() {
    return new WorkerLeases(
        directory, settings.leaseDuration(), IdLayout.seatWave(), Clock.systemUTC());
  }

  /** Registers customers fan_1 to fan_N, each on its own shard, with a freshly leased worker id. */
  List<Long> registerCustomers(String holder, int count) throws SQLException, InterruptedException {
    WorkerLease lease = leases().take(holder).discardError().orElseThrow();
    Customers customers =
        new Customers(
            router,
            new LeasedIdGenerator(
                IdLayout.seatWave(), lease, Clock.systemUTC(), BACKWARD_TOLERANCE));
    List<Long> customerIds = new ArrayList<>(count);
    for (int fan = 1; fan <= count; fan++) {
      Result<Long, WriteRefusal> registered = customers.register("fan_" + fan);
      customerIds.add(registered.discardError().orElseThrow());
    }
    return List.copyOf(customerIds);
  }

  /** Counts rows with one query on one shard, as the owner; the SQL text is the test's own. */
  long countOn(ShardAddress shard, String sql, Object... parameters) throws SQLException {
    return countOn(shard.hostName(), shard.schemaName(), sql, parameters);
  }

  /** Counts rows with one query in one schema on one machine, as the owner. */
  long countOn(String hostName, String schema, String sql, Object... parameters)
      throws SQLException {
    try (Connection connection = cluster.owner(hostName).getConnection()) {
      connection.setSchema(schema);
      try (PreparedStatement query = connection.prepareStatement(sql)) {
        for (int index = 0; index < parameters.length; index++) {
          query.setObject(index + 1, parameters[index]);
        }
        try (ResultSet rows = query.executeQuery()) {
          rows.next();
          return rows.getLong(1);
        }
      }
    }
  }

  /** Runs one owner statement on one machine, for a test's own set-up. */
  void runAsOwner(String hostName, String sql, Object... parameters) throws SQLException {
    try (Connection connection = cluster.owner(hostName).getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int index = 0; index < parameters.length; index++) {
        statement.setObject(index + 1, parameters[index]);
      }
      statement.execute();
    }
  }

  /** Runs one owner statement on the directory. */
  void runOnDirectory(String sql, Object... parameters) throws SQLException {
    try (Connection connection = cluster.directoryOwner().getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int index = 0; index < parameters.length; index++) {
        statement.setObject(index + 1, parameters[index]);
      }
      statement.execute();
    }
  }

  @Override
  public void close() {
    directory.close();
    router.close();
    cluster.close();
  }
}
