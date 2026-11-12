package io.plainround.seatwave.partitions.stack;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sviperll.result4j.Result;
import io.plainround.seatwave.partitions.ids.LeaseRefusal;
import io.plainround.seatwave.partitions.ids.WorkerLease;
import io.plainround.seatwave.partitions.ids.WorkerLeases;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
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
class WorkerLeaseStackTest {

  // More racers than the 24 connections of the directory's pool, so some wait for a connection
  // and the grants overlap in every order.
  private static final int RACERS = 32;
  private static final Duration PATIENCE = Duration.ofSeconds(30);
  private static StackLab lab;

  @BeforeAll
  static void openLab() throws IOException, SQLException {
    lab = StackLab.open();
  }

  @AfterAll
  static void closeLab() {
    lab.close();
  }

  @BeforeEach
  void freeEveryWorkerId() throws SQLException {
    lab.runOnDirectory("DELETE FROM directory.id_worker_leases");
  }

  @Test
  void racingInstancesNeverShareAWorkerId() throws Exception {
    WorkerLeases leases = lab.leases();
    CyclicBarrier start = new CyclicBarrier(RACERS);
    List<Future<Result<WorkerLease, LeaseRefusal>>> taken = new ArrayList<>();
    try (ExecutorService racers = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int racer = 0; racer < RACERS; racer++) {
        String holder = "instance-" + racer;
        taken.add(
            racers.submit(() -> {
              start.await(PATIENCE.toSeconds(), TimeUnit.SECONDS);
              return leases.take(holder);
            }));
      }
    }
    List<Integer> workerIds = new ArrayList<>();
    for (Future<Result<WorkerLease, LeaseRefusal>> lease : taken) {
      workerIds.add(lease.get().discardError().orElseThrow().workerId());
    }

    assertThat(workerIds).doesNotHaveDuplicates().hasSize(RACERS);
    assertThat(countLeases()).isEqualTo(RACERS);
  }

  @Test
  void aLiveLeaseIsNotGivenToASecondHolder() throws SQLException {
    WorkerLeases leases = lab.leases();
    WorkerLease first = leases.take("instance-a").discardError().orElseThrow();

    WorkerLease second = leases.take("instance-b").discardError().orElseThrow();

    assertThat(second.workerId()).isNotEqualTo(first.workerId());
  }

  @Test
  void anExpiredLeaseGoesToTheNextHolderAndTheOldHolderCannotRenewIt() throws SQLException {
    WorkerLeases leases = lab.leases();
    WorkerLease old = leases.take("instance-a").discardError().orElseThrow();
    lab.runOnDirectory(
        "UPDATE directory.id_worker_leases SET expires_at = now() - interval '1 second'");

    WorkerLease next = leases.take("instance-b").discardError().orElseThrow();

    assertThat(next.workerId()).isEqualTo(old.workerId());
    assertThat(leases.renew(old)).isEqualTo(Result.error(LeaseRefusal.LEASE_LOST));
  }

  @Test
  void aRenewalKeepsTheIdAndEndsLaterThanTheGrant() throws SQLException {
    WorkerLeases leases = lab.leases();
    WorkerLease granted = leases.take("instance-a").discardError().orElseThrow();

    WorkerLease renewed = leases.renew(granted).discardError().orElseThrow();

    assertThat(renewed.workerId()).isEqualTo(granted.workerId());
    assertThat(renewed.validUntil()).isAfterOrEqualTo(granted.validUntil());
  }

  @Test
  void aReleasedIdIsFreeAtOnce() throws SQLException {
    WorkerLeases leases = lab.leases();
    WorkerLease released = leases.take("instance-a").discardError().orElseThrow();

    assertThat(leases.release(released)).isTrue();

    assertThat(leases.take("instance-b").discardError().orElseThrow().workerId())
        .isEqualTo(released.workerId());
  }

  private static long countLeases() throws SQLException {
    try (var connection = lab.cluster.directoryOwner().getConnection();
        var query =
            connection.prepareStatement(
                "SELECT count(DISTINCT holder) FROM directory.id_worker_leases");
        var rows = query.executeQuery()) {
      rows.next();
      return rows.getLong(1);
    }
  }
}
