package io.plainround.seatwave.partitions.move;

import com.github.sviperll.result4j.Result;
import com.zaxxer.hikari.HikariDataSource;
import io.plainround.seatwave.partitions.config.Databases;
import io.plainround.seatwave.partitions.config.SettingsReader;
import io.plainround.seatwave.partitions.config.StackSettings;
import io.plainround.seatwave.partitions.holds.Customers;
import io.plainround.seatwave.partitions.holds.HoldFlow;
import io.plainround.seatwave.partitions.ids.IdLayout;
import io.plainround.seatwave.partitions.ids.LeaseRefusal;
import io.plainround.seatwave.partitions.ids.LeasedIdGenerator;
import io.plainround.seatwave.partitions.ids.WorkerLease;
import io.plainround.seatwave.partitions.ids.WorkerLeases;
import io.plainround.seatwave.partitions.lab.LabCluster;
import io.plainround.seatwave.partitions.lab.SectionKey;
import io.plainround.seatwave.partitions.routing.ShardKeys;
import io.plainround.seatwave.partitions.routing.ShardRouter;
import io.plainround.seatwave.partitions.routing.WriteRefusal;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * The measured shard move: loads {@code shard-17} on its first machine, moves it to the second by
 * logical replication while a writer sends holds and confirms to it, and prints the copy speed, the
 * catch-up, the pause at the switch, the audit of every acknowledged write, a stale router's
 * refused write, and the module's full-size arithmetic from the measured rate. It starts from an
 * empty lab ({@link LabCluster#reset()}) so a second run measures the same thing. Run with {@code
 * ./gradlew shardMove} after {@code scripts/stack.sh up --run}.
 */
public final class ShardMoveCli {

  private static final String SHARD = "shard-17";
  // Section 6e's shard at the lifetime volume, and the module's assumptions it is compared with.
  private static final double FULL_SHARD_BYTES = 312.5e9;
  private static final double ASSUMED_COPY_BYTES_PER_SECOND = 250e6;
  private static final double ASSUMED_PAUSE_SECONDS = 0.6;
  private static final double BYTES_PER_MEGABYTE = 1e6;
  private static final double NANOS_PER_SECOND = 1e9;
  private static final double SECONDS_PER_MINUTE = 60;
  private static final double MILLIS_PER_SECOND = 1_000;
  // The writer runs this long before the move starts and after it ends, so writes overlap every
  // step of the move, the switch included.
  private static final long WRITER_MARGIN_MILLIS = 3_000;
  // Module 18's generator: 10 ms of backward clock tolerance.
  private static final Duration BACKWARD_TOLERANCE = Duration.ofMillis(10);

  private ShardMoveCli() {}

  /**
   * Runs the move.
   *
   * @param arguments one argument: the settings file
   * @throws Exception if the lab is not up or a step fails; the message says which
   */
  public static void main(String[] arguments) throws Exception {
    if (arguments.length != 1) {
      System.err.println("usage: ShardMoveCli <settings file>");
      System.exit(2);
    }
    StackSettings settings = read(Path.of(arguments[0]));
    String fromHost = settings.hostNames().get(0);
    String toHost = settings.hostNames().get(1);
    try (LabCluster cluster = LabCluster.prepare(settings);
        ShardRouter router = ShardRouter.open(settings, "router-a")) {
      cluster.reset();
      router.reload();
      System.out.printf(
          Locale.ROOT,
          "Shard move of %s from %s to %s (ring version %d, 64 shards, 1,024 points each)%n",
          SHARD,
          fromHost,
          toHost,
          router.ringVersion());
      List<SectionKey> sections = ShardSeed.sectionsOn(router, SHARD, settings.seedSections());
      long seedStarted = System.nanoTime();
      long seededHolds = ShardSeed.seed(cluster, router, sections);
      long[] sizes = ShardMover.sizes(cluster.owner(fromHost), "shard_17");
      System.out.printf(
          Locale.ROOT,
          "SEED   %,d sections on %s: %,d seats, %,d holds and %,d bookings in %.1f s%n"
              + "       its tables on %s: %,.0f MB without indexes, %,.0f MB with indexes%n",
          sections.size(),
          SHARD,
          (long) sections.size() * LabCluster.SEATS_PER_SECTION,
          seededHolds,
          seededHolds,
          seconds(System.nanoTime() - seedStarted),
          fromHost,
          sizes[0] / BYTES_PER_MEGABYTE,
          sizes[1] / BYTES_PER_MEGABYTE);
      try (ShardRouter staleRouter = ShardRouter.open(settings, "router-b")) {
        leaseAndMove(cluster, router, staleRouter, sections, seededHolds);
      }
    }
  }

  private static void leaseAndMove(
      LabCluster cluster,
      ShardRouter router,
      ShardRouter staleRouter,
      List<SectionKey> sections,
      long seededHolds)
      throws IOException, SQLException, InterruptedException {
    StackSettings settings = cluster.settings();
    try (HikariDataSource directory =
        Databases.pool(settings, settings.directoryUrl(), Databases.Role.APPLICATION, "leases")) {
      WorkerLeases leases =
          new WorkerLeases(
              directory, settings.leaseDuration(), IdLayout.seatWave(), Clock.systemUTC());
      WorkerLease lease = take(leases);
      runMove(cluster, router, staleRouter, sections, seededHolds, lease);
      leases.release(lease);
    }
  }

  private static void runMove(
      LabCluster cluster,
      ShardRouter router,
      ShardRouter staleRouter,
      List<SectionKey> sections,
      long seededHolds,
      WorkerLease lease)
      throws IOException, SQLException, InterruptedException {
    StackSettings settings = cluster.settings();
    LeasedIdGenerator ids =
        new LeasedIdGenerator(IdLayout.seatWave(), lease, Clock.systemUTC(), BACKWARD_TOLERANCE);
    long registerStarted = System.nanoTime();
    List<Long> customers = register(new Customers(router, ids), settings.writerCustomers());
    System.out.printf(
        Locale.ROOT,
        "WRITER %,d customers registered in %.1f s (worker id %d); %d holds a second, each"
            + " confirmed, on the free seats of %s%n",
        customers.size(),
        seconds(System.nanoTime() - registerStarted),
        lease.workerId(),
        settings.writerHoldsPerSecond(),
        SHARD);
    HoldFlow flow = new HoldFlow(router, settings.holdDuration());
    MoveReport report;
    WriterTally tally;
    try (MoveWriter writer =
        new MoveWriter(flow, customers, sections, settings.writerHoldsPerSecond())) {
      writer.start();
      TimeUnit.MILLISECONDS.sleep(WRITER_MARGIN_MILLIS);
      report =
          new ShardMover(cluster, router)
              .move(SHARD, cluster.initialHostOf(SHARD), settings.hostNames().get(1));
      TimeUnit.MILLISECONDS.sleep(WRITER_MARGIN_MILLIS);
      tally = writer.stop();
    }
    printMove(report);
    printWrites(tally, MoveAudit.of(cluster, report, seededHolds, tally));
    printStaleRouter(staleRouter, settings, customers, sections);
    printFullSize(report);
  }

  private static void printMove(MoveReport report) {
    System.out.printf(
        Locale.ROOT,
        "MOVE   copy:     %.2f s, %.1f MB/s of table data%n"
            + "       catch-up: %.0f ms (a marker written after the copy, until visible on %s)%n"
            + "       pause:    %.1f ms (fence %.1f ms, lag to 0 %.1f ms, directory flip and"
            + " reload %.1f ms)%n"
            + "       the copy on %s with its indexes: %,.0f MB%n",
        seconds(report.copy().toNanos()),
        report.copyBytesPerSecond() / BYTES_PER_MEGABYTE,
        millis(report.catchUp()),
        report.toHost(),
        millis(report.pause()),
        millis(report.fence()),
        millis(report.lagZero()),
        millis(report.pause().minus(report.fence()).minus(report.lagZero())),
        report.toHost(),
        report.targetTotalBytes() / BYTES_PER_MEGABYTE);
  }

  private static void printWrites(WriterTally tally, MoveAudit audit) {
    System.out.printf(
        Locale.ROOT,
        "WRITES sent %,d; holds acknowledged %,d; bookings acknowledged %,d; refusals %s;"
            + " failures %d%n"
            + "AUDIT  lost %d, doubled %d%n"
            + "       holds on the new machine %,d = seed %,d + acknowledged %,d%n"
            + "       bookings on the new machine %,d = seed %,d + acknowledged %,d%n"
            + "       acknowledged ids found there: %,d of %,d holds, %,d of %,d bookings%n"
            + "       holds left on the old machine %,d; writer holds there but not on the new"
            + " one: %d%n",
        tally.sent(),
        tally.heldIds().size(),
        tally.bookedIds().size(),
        tally.refusals().isEmpty() ? "none" : tally.refusals(),
        tally.failures(),
        audit.lost(),
        audit.doubled(),
        audit.targetHolds(),
        audit.seededHolds(),
        audit.ackedHolds(),
        audit.targetBookings(),
        audit.seededHolds(),
        audit.ackedBookings(),
        audit.ackedHoldsFound(),
        audit.ackedHolds(),
        audit.ackedBookingsFound(),
        audit.ackedBookings(),
        audit.sourceHolds(),
        audit.sourceOnlyHolds());
  }

  private static void printStaleRouter(
      ShardRouter staleRouter,
      StackSettings settings,
      List<Long> customers,
      List<SectionKey> sections)
      throws SQLException, InterruptedException {
    HoldFlow staleFlow = new HoldFlow(staleRouter, settings.holdDuration());
    // A customer whose own shard is not the moved one, so the refusal comes from the claim.
    long customerId =
        customers.stream()
            .filter(id -> !staleRouter.route(ShardKeys.customer(id)).shardName().equals(SHARD))
            .findFirst()
            .orElseThrow();
    SectionKey section = sections.getFirst();
    String seat = section.seatLabel(LabCluster.SEATS_PER_SECTION);
    Result<UUID, WriteRefusal> refused = staleFlow.hold(customerId, section.eventId(), seat);
    System.out.printf(
        Locale.ROOT,
        "STALE  a router that read the directory before the move sends a hold on %s %s to %s:%n"
            + "       %s%n",
        section.shardKey(),
        seat,
        staleRouter.route(section.shardKey()).hostName(),
        refused);
    staleRouter.reload();
    Result<UUID, WriteRefusal> accepted = staleFlow.hold(customerId, section.eventId(), seat);
    System.out.printf(
        Locale.ROOT,
        "       after its reload it sends the same hold to %s: %s%n",
        staleRouter.route(section.shardKey()).hostName(),
        accepted.isError() ? accepted : "accepted");
  }

  private static void printFullSize(MoveReport report) {
    double measuredRate = report.copyBytesPerSecond();
    System.out.printf(
        Locale.ROOT,
        "FULL SIZE, the module's 312.5 GB shard (table data) at this run's rate:%n"
            + "       %.1f MB/s is %.2f times the assumed 250 MB/s; copy %.0f minutes (assumed %.0f)%n"
            + "       pause %.3f s against the assumed %.1f s%n",
        measuredRate / BYTES_PER_MEGABYTE,
        measuredRate / ASSUMED_COPY_BYTES_PER_SECOND,
        FULL_SHARD_BYTES / measuredRate / SECONDS_PER_MINUTE,
        FULL_SHARD_BYTES / ASSUMED_COPY_BYTES_PER_SECOND / SECONDS_PER_MINUTE,
        report.pause().toNanos() / NANOS_PER_SECOND,
        ASSUMED_PAUSE_SECONDS);
  }

  private static WorkerLease take(WorkerLeases leases) throws SQLException {
    Result<WorkerLease, LeaseRefusal> taken = leases.take("shard-move-cli");
    if (taken instanceof Result.Success<WorkerLease, LeaseRefusal>(WorkerLease lease)) {
      return lease;
    }
    throw new IllegalStateException("no worker id could be leased: " + taken);
  }

  private static List<Long> register(Customers registry, int count)
      throws SQLException, InterruptedException {
    List<Long> customers = new ArrayList<>(count);
    for (int fan = 1; fan <= count; fan++) {
      Result<Long, WriteRefusal> registered = registry.register("fan_" + fan);
      if (!(registered instanceof Result.Success<Long, WriteRefusal>(Long customerId))) {
        throw new IllegalStateException("fan_" + fan + " was not registered: " + registered);
      }
      customers.add(customerId);
    }
    return List.copyOf(customers);
  }

  private static StackSettings read(Path file) throws IOException {
    return switch (StackSettings.read(new SettingsReader(SettingsReader.load(file)))) {
      case Result.Success<StackSettings, List<String>>(StackSettings settings) -> settings;
      case Result.Error<StackSettings, List<String>>(List<String> problems) ->
          throw new IllegalStateException(
              "the settings in " + file + " have problems: " + problems);
    };
  }

  private static double seconds(long nanos) {
    return nanos / NANOS_PER_SECOND;
  }

  private static double millis(Duration duration) {
    return duration.toNanos() / NANOS_PER_SECOND * MILLIS_PER_SECOND;
  }
}
