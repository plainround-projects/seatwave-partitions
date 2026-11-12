package io.plainround.seatwave.partitions.stack;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.sviperll.result4j.Result;
import io.plainround.seatwave.partitions.holds.HoldFlow;
import io.plainround.seatwave.partitions.lab.LabCluster;
import io.plainround.seatwave.partitions.lab.SectionKey;
import io.plainround.seatwave.partitions.move.CopyInProgress;
import io.plainround.seatwave.partitions.move.MoveAudit;
import io.plainround.seatwave.partitions.move.MoveReport;
import io.plainround.seatwave.partitions.move.MoveWriter;
import io.plainround.seatwave.partitions.move.ShardMover;
import io.plainround.seatwave.partitions.move.ShardSeed;
import io.plainround.seatwave.partitions.move.WriterTally;
import io.plainround.seatwave.partitions.routing.ShardKeys;
import io.plainround.seatwave.partitions.routing.ShardRouter;
import io.plainround.seatwave.partitions.routing.WriteRefusal;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("stack")
class ShardMoveStackTest {

  private static final String SHARD = "shard-17";
  private static final String SCHEMA = "shard_17";
  private static final int SECTIONS = 20;
  private static final int FANS = 200;
  private static final int HOLDS_PER_SECOND = 100;
  // Writes run this long before the move and after it, so they overlap the copy and the switch.
  private static final long MARGIN_MILLIS = 1_000;
  private static final Duration PATIENCE = Duration.ofSeconds(60);
  private static StackLab lab;
  private String fromHost;
  private String toHost;
  private List<SectionKey> sections;
  private long seeded;

  @BeforeAll
  static void openLab() throws IOException, SQLException {
    lab = StackLab.open();
  }

  @AfterAll
  static void closeLab() throws SQLException {
    // The next test class expects every shard on its first machine.
    lab.cluster.reset();
    lab.close();
  }

  @BeforeEach
  void seedTheShard() throws SQLException {
    lab.cluster.reset();
    lab.router.reload();
    fromHost = lab.cluster.initialHostOf(SHARD);
    toHost = lab.settings.hostNames().get(1);
    sections = ShardSeed.sectionsOn(lab.router, SHARD, SECTIONS);
    seeded = ShardSeed.seed(lab.cluster, lab.router, sections);
  }

  @Test
  void aShardMovesUnderWritesWithNoWriteLostOrDoubled() throws Exception {
    List<Long> fans = lab.registerCustomers("move-test", FANS);
    HoldFlow flow = new HoldFlow(lab.router, lab.settings.holdDuration());
    MoveReport report;
    WriterTally tally;
    try (MoveWriter writer = new MoveWriter(flow, fans, sections, HOLDS_PER_SECOND)) {
      writer.start();
      TimeUnit.MILLISECONDS.sleep(MARGIN_MILLIS);
      report = new ShardMover(lab.cluster, lab.router).move(SHARD, fromHost, toHost);
      TimeUnit.MILLISECONDS.sleep(MARGIN_MILLIS);
      tally = writer.stop();
    }

    MoveAudit audit = MoveAudit.of(lab.cluster, report, seeded, tally);
    assertThat(tally.failures()).isZero();
    assertThat(tally.heldIds()).isNotEmpty();
    assertThat(audit.lost()).isZero();
    assertThat(audit.doubled()).isZero();
    assertThat(lab.router.route(sections.getFirst().shardKey()).hostName()).isEqualTo(toHost);
  }

  @Test
  void aStaleRoutersWriteToTheOldMachineIsRefusedAfterTheSwitch() throws Exception {
    // A fan whose own shard is not the moved one, so the refusal comes from the seat's claim.
    long fan =
        lab.registerCustomers("stale-test", 3).stream()
            .filter(id -> !lab.router.route(ShardKeys.customer(id)).shardName().equals(SHARD))
            .findFirst()
            .orElseThrow();
    try (ShardRouter stale = ShardRouter.open(lab.settings, "stale-router")) {
      new ShardMover(lab.cluster, lab.router).move(SHARD, fromHost, toHost);
      String seat = sections.getFirst().seatLabel(LabCluster.SEATS_PER_SECTION);

      Result<UUID, WriteRefusal> refused =
          new HoldFlow(stale, lab.settings.holdDuration())
              .hold(fan, sections.getFirst().eventId(), seat);

      assertThat(refused)
          .isEqualTo(
              Result.error(new WriteRefusal.NotOwner(stale.route(sections.getFirst().shardKey()))));
      assertThat(stale.route(sections.getFirst().shardKey()).hostName()).isEqualTo(fromHost);
    }
  }

  @Test
  void theFenceWaitsForAWriteInFlightSoTheWriteIsCopiedBeforeTheSwitch() throws Exception {
    ShardMover mover = new ShardMover(lab.cluster, lab.router);
    CopyInProgress copy = mover.copy(SHARD, fromHost, toHost);
    SectionKey section = sections.getFirst();
    UUID inFlight = UUID.randomUUID();
    try (Connection stale = lab.cluster.owner(fromHost).getConnection();
        ExecutorService switcher = Executors.newVirtualThreadPerTaskExecutor()) {
      // A write from a router that never saw the pause: it passed the ownership check and has not
      // committed when the switch starts.
      stale.setSchema(SCHEMA);
      stale.setAutoCommit(false);
      insertHold(stale, inFlight, section);
      Future<MoveReport> finished = switcher.submit(() -> mover.finish(copy));
      // The correct fence blocks on this transaction's lock; without the lock, the switch ends.
      await().atMost(PATIENCE).until(() -> finished.isDone() || fenceIsWaitingForALock());
      stale.commit();
      finished.get(PATIENCE.toSeconds(), TimeUnit.SECONDS);
    }

    assertThat(
            lab.countOn(toHost, SCHEMA, "SELECT count(*) FROM holds WHERE hold_id = ?", inFlight))
        .isEqualTo(1);
  }

  private static void insertHold(Connection connection, UUID holdId, SectionKey section)
      throws SQLException {
    try (PreparedStatement insert =
        connection.prepareStatement(
            """
            INSERT INTO holds (hold_id, event_id, seat_label, customer_id, state, created_at,
                               expires_at)
            VALUES (?, ?, ?, 1, 'ACTIVE', now(), now() + interval '300 seconds')
            """)) {
      insert.setObject(1, holdId);
      insert.setLong(2, section.eventId());
      insert.setString(3, section.seatLabel(LabCluster.SEATS_PER_SECTION));
      insert.executeUpdate();
    }
  }

  private boolean fenceIsWaitingForALock() throws SQLException {
    return lab.countOn(
            fromHost,
            "routing",
            "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'"
                + " AND query LIKE 'LOCK TABLE%'")
        > 0;
  }
}
