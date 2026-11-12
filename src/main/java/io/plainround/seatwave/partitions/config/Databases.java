package io.plainround.seatwave.partitions.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;

/**
 * Opens connection pools to the lab's databases and migrates their schemas. Pools are created once
 * by their owner (a router, a mover, a test) and closed by it. Stateless and thread-safe.
 */
public final class Databases {

  /** The directory's schema: the ring and the worker-id leases. */
  public static final String DIRECTORY_SCHEMA = "directory";

  /** The schema on every shard machine that says which shards it serves. */
  public static final String ROUTING_SCHEMA = "routing";

  // Set-up, clean-up and the shard move's steps run one statement at a time.
  private static final int OPERATOR_POOL_SIZE = 4;

  private Databases() {}

  /** Which role a pool logs in as. */
  public enum Role {
    /** The superuser: only the shard move's publication and subscription need it. */
    SUPERUSER,
    /** Owns the schemas: migrations, set-up, the directory flip and clean-up. */
    OWNER,
    /** SeatWave itself: rows only. */
    APPLICATION
  }

  /** Which set of migrations a schema gets. */
  public enum Migrations {
    /** {@code db/directory}: ring_versions, ring_shards, id_worker_leases. */
    DIRECTORY("classpath:db/directory"),
    /** {@code db/host}: served_shards and the ownership check, once per shard machine. */
    HOST("classpath:db/host"),
    /** {@code db/shard}: Module 13's tables and Module 18's, once per shard schema. */
    SHARD("classpath:db/shard");

    private final String location;

    Migrations(String location) {
      this.location = location;
    }
  }

  /**
   * Migrates one schema with Flyway as the owner role, creating it if it does not exist. PostgreSQL
   * runs DDL inside transactions, so a migration that fails leaves nothing half done.
   *
   * @param settings the settings
   * @param jdbcUrl the database
   * @param schema the schema to migrate
   * @param migrations which migrations it gets
   * @return Flyway's result, whose {@code migrationsExecuted} is 0 when the schema was current
   * @throws IOException if the owner's password file cannot be read
   */
  public static MigrateResult migrate(
      StackSettings settings, String jdbcUrl, String schema, Migrations migrations)
      throws IOException {
    Objects.requireNonNull(settings, "settings");
    Objects.requireNonNull(migrations, "migrations");
    return Flyway.configure()
        .dataSource(
            Objects.requireNonNull(jdbcUrl, "jdbcUrl"),
            settings.ownerUser(),
            password(settings.ownerPasswordFile()))
        .schemas(Objects.requireNonNull(schema, "schema"))
        .locations(migrations.location)
        .placeholders(
            Map.of(
                "appRole", settings.appUser(),
                "replicatorRole", settings.replicatorUser(),
                "routingSchema", ROUTING_SCHEMA))
        .load()
        .migrate();
  }

  /**
   * Opens a pool to one database.
   *
   * @param settings the settings
   * @param jdbcUrl the database
   * @param role the role every connection logs in as
   * @param poolName a name for the pool's log lines
   * @return the pool; the caller closes it
   * @throws IOException if the password file cannot be read
   */
  public static HikariDataSource pool(
      StackSettings settings, String jdbcUrl, Role role, String poolName) throws IOException {
    Objects.requireNonNull(settings, "settings");
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(Objects.requireNonNull(jdbcUrl, "jdbcUrl"));
    config.setUsername(user(settings, role));
    config.setPassword(password(passwordFile(settings, role)));
    config.setMaximumPoolSize(role == Role.APPLICATION ? settings.poolSize() : OPERATOR_POOL_SIZE);
    config.setConnectionTimeout(settings.connectTimeout().toMillis());
    // Transactions set autocommit explicitly; the pool restores it, and the schema, on return.
    config.setAutoCommit(true);
    config.setPoolName(role.name().toLowerCase(Locale.ROOT) + "-" + poolName);
    return new HikariDataSource(config);
  }

  /**
   * Reads a generated password file.
   *
   * @param file the file
   * @return its content without surrounding white space
   * @throws IOException if it cannot be read; the message says which file and how to make it
   */
  public static String password(Path file) throws IOException {
    try {
      return Files.readString(file, StandardCharsets.UTF_8).strip();
    } catch (IOException unreadable) {
      throw new IOException(
          "the database password file "
              + file
              + " cannot be read; scripts/stack.sh up generates it",
          unreadable);
    }
  }

  private static String user(StackSettings settings, Role role) {
    return switch (role) {
      case SUPERUSER -> settings.superUser();
      case OWNER -> settings.ownerUser();
      case APPLICATION -> settings.appUser();
    };
  }

  private static Path passwordFile(StackSettings settings, Role role) {
    return switch (role) {
      case SUPERUSER -> settings.superPasswordFile();
      case OWNER -> settings.ownerPasswordFile();
      case APPLICATION -> settings.appPasswordFile();
    };
  }
}
