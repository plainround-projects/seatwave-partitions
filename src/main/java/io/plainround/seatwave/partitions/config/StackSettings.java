package io.plainround.seatwave.partitions.config;

import com.github.sviperll.result4j.Result;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Everything the stack tests and the shard move read from {@code config/stack.properties}, checked
 * once. Immutable and thread-safe.
 *
 * @param directoryUrl the directory database, on the loopback address
 * @param hostUrls each shard machine's name (as the directory's {@code ring_shards.host_name} and
 *     the Compose service name) and its database, on the loopback address, in file order
 * @param superUser the superuser, for the shard move's publication and subscription only
 * @param superPasswordFile where the superuser's generated password is
 * @param ownerUser the role that owns the schemas and runs migrations
 * @param ownerPasswordFile where the owner's generated password is
 * @param appUser the role SeatWave runs as: rows only, no DDL
 * @param appPasswordFile where the application's generated password is
 * @param replicatorUser the role a subscription logs in as to copy a shard
 * @param replicatorPasswordFile where the replicator's generated password is
 * @param poolSize connections in each application pool, one pool per machine
 * @param connectTimeout the longest wait for a pooled connection
 * @param pauseWait the longest a write waits for a paused shard before it is refused
 * @param holdDuration how long a hold (and its reservation) lasts
 * @param leaseDuration how long a worker-id lease lasts without renewal
 * @param copyTimeout the longest the shard move waits for its initial copy
 * @param lagTimeout the longest the shard move waits for the copy to catch up at the switch
 * @param seedSections sections of 1,000 seats the shard move loads onto the moving shard
 * @param writerHoldsPerSecond holds a second the shard move's writer sends while it runs
 * @param writerCustomers customers the writer's holds are spread over
 */
public record StackSettings(
    String directoryUrl,
    Map<String, String> hostUrls,
    String superUser,
    Path superPasswordFile,
    String ownerUser,
    Path ownerPasswordFile,
    String appUser,
    Path appPasswordFile,
    String replicatorUser,
    Path replicatorPasswordFile,
    int poolSize,
    Duration connectTimeout,
    Duration pauseWait,
    Duration holdDuration,
    Duration leaseDuration,
    Duration copyTimeout,
    Duration lagTimeout,
    int seedSections,
    int writerHoldsPerSecond,
    int writerCustomers) {

  // The lab databases only: PostgreSQL on the loopback address, a port in Module 18's 21800 to
  // 21899.
  private static final Pattern LOOPBACK_POSTGRES =
      Pattern.compile("jdbc:postgresql://127\\.0\\.0\\.1:218[0-9]{2}/[a-z_]{1,32}");
  private static final Pattern ROLE = Pattern.compile("[a-z_]{1,32}");
  // The same alphabet as ring_shards.host_name's use as a Compose service name.
  private static final Pattern HOST_NAME = Pattern.compile("[a-z0-9-]{1,64}");
  // HikariCP refuses a connection timeout below 250 ms; checking it here turns that into a
  // settings problem instead of a stack trace from the pool's constructor.
  private static final int HIKARI_MIN_CONNECT_TIMEOUT_MILLIS = 250;
  private static final int MAX_WAIT_MILLIS = 120_000;
  // compose.yaml sets max_connections to 200 on each shard machine; a test opens at most two
  // routers' pools and two owner pools there.
  private static final int MAX_POOL_SIZE = 64;
  private static final int MAX_HOLD_SECONDS = 3_600;
  private static final int MAX_LEASE_SECONDS = 3_600;
  private static final int MAX_COPY_SECONDS = 7_200;
  // 25,000 sections of 1,000 seats is 25,000,000 bookings, about 10 times the measured run; more
  // would not fit the 20 GB the course keeps free inside Docker's virtual machine.
  private static final int MAX_SEED_SECTIONS = 25_000;
  private static final int MAX_WRITER_RATE = 5_000;
  private static final int MAX_WRITER_CUSTOMERS = 1_000_000;

  /** Checks that nothing is missing and copies the host map. */
  public StackSettings {
    Objects.requireNonNull(directoryUrl, "directoryUrl");
    // A copy that keeps file order, so hostNames() lists the machines as the file does.
    hostUrls =
        Collections.unmodifiableMap(
            new LinkedHashMap<>(Objects.requireNonNull(hostUrls, "hostUrls")));
    Objects.requireNonNull(superUser, "superUser");
    Objects.requireNonNull(superPasswordFile, "superPasswordFile");
    Objects.requireNonNull(ownerUser, "ownerUser");
    Objects.requireNonNull(ownerPasswordFile, "ownerPasswordFile");
    Objects.requireNonNull(appUser, "appUser");
    Objects.requireNonNull(appPasswordFile, "appPasswordFile");
    Objects.requireNonNull(replicatorUser, "replicatorUser");
    Objects.requireNonNull(replicatorPasswordFile, "replicatorPasswordFile");
    Objects.requireNonNull(connectTimeout, "connectTimeout");
    Objects.requireNonNull(pauseWait, "pauseWait");
    Objects.requireNonNull(holdDuration, "holdDuration");
    Objects.requireNonNull(leaseDuration, "leaseDuration");
    Objects.requireNonNull(copyTimeout, "copyTimeout");
    Objects.requireNonNull(lagTimeout, "lagTimeout");
  }

  /**
   * Reads and checks every key, and reports all problems together.
   *
   * @param reader the loaded file
   * @return the settings, or one line per problem
   */
  public static Result<StackSettings, List<String>> read(SettingsReader reader) {
    Objects.requireNonNull(reader, "reader");
    String directoryUrl = reader.textMatching("directory.url", LOOPBACK_POSTGRES);
    Map<String, String> hostUrls = new LinkedHashMap<>();
    for (String host : reader.listMatching("hosts", HOST_NAME)) {
      hostUrls.put(host, reader.textMatching("host." + host + ".url", LOOPBACK_POSTGRES));
    }
    String superUser = reader.textMatching("db.superUser", ROLE);
    String superPasswordFile = reader.filePath("db.superPasswordFile");
    String ownerUser = reader.textMatching("db.ownerUser", ROLE);
    String ownerPasswordFile = reader.filePath("db.ownerPasswordFile");
    String appUser = reader.textMatching("db.appUser", ROLE);
    String appPasswordFile = reader.filePath("db.appPasswordFile");
    String replicatorUser = reader.textMatching("db.replicatorUser", ROLE);
    String replicatorPasswordFile = reader.filePath("db.replicatorPasswordFile");
    int poolSize = reader.intBetween("db.poolSize", 1, MAX_POOL_SIZE);
    Duration connectTimeout =
        reader.millisBetween(
            "db.connectTimeoutMillis", HIKARI_MIN_CONNECT_TIMEOUT_MILLIS, MAX_WAIT_MILLIS);
    Duration pauseWait = reader.millisBetween("router.pauseWaitMillis", 1, MAX_WAIT_MILLIS);
    Duration holdDuration =
        Duration.ofSeconds(reader.intBetween("hold.durationSeconds", 1, MAX_HOLD_SECONDS));
    Duration leaseDuration =
        Duration.ofSeconds(reader.intBetween("lease.durationSeconds", 1, MAX_LEASE_SECONDS));
    Duration copyTimeout =
        Duration.ofSeconds(reader.intBetween("move.copyTimeoutSeconds", 1, MAX_COPY_SECONDS));
    Duration lagTimeout = reader.millisBetween("move.lagTimeoutMillis", 1, MAX_WAIT_MILLIS);
    int seedSections = reader.intBetween("seed.sections", 1, MAX_SEED_SECTIONS);
    int writerRate = reader.intBetween("writer.holdsPerSecond", 1, MAX_WRITER_RATE);
    int writerCustomers = reader.intBetween("writer.customers", 1, MAX_WRITER_CUSTOMERS);
    // A move needs a machine to move from and one to move to; the cross-key rule runs only when
    // the list itself was read cleanly, so one bad key gives one problem line.
    boolean hostsReadCleanly =
        reader.problems().stream().noneMatch(problem -> problem.startsWith("hosts "));
    if (hostsReadCleanly && hostUrls.size() < 2) {
      reader.addProblem(
          "hosts must name at least two shard machines, but named " + hostUrls.size());
    }
    if (!reader.problems().isEmpty()) {
      return Result.error(reader.problems());
    }
    return Result.success(
        new StackSettings(
            directoryUrl,
            hostUrls,
            superUser,
            Path.of(superPasswordFile),
            ownerUser,
            Path.of(ownerPasswordFile),
            appUser,
            Path.of(appPasswordFile),
            replicatorUser,
            Path.of(replicatorPasswordFile),
            poolSize,
            connectTimeout,
            pauseWait,
            holdDuration,
            leaseDuration,
            copyTimeout,
            lagTimeout,
            seedSections,
            writerRate,
            writerCustomers));
  }

  /** Returns the shard machines' names in file order. */
  public List<String> hostNames() {
    return List.copyOf(hostUrls.keySet());
  }
}
