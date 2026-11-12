package io.plainround.seatwave.partitions.stack;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.github.sviperll.result4j.Result;
import io.plainround.seatwave.partitions.routing.Constraints;
import io.plainround.seatwave.partitions.routing.ShardAddress;
import io.plainround.seatwave.partitions.routing.ShardKeys;
import io.plainround.seatwave.partitions.routing.ShardPause;
import io.plainround.seatwave.partitions.routing.WriteRefusal;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
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
class ShardRouterStackTest {

  private static final Duration PATIENCE = Duration.ofSeconds(10);
  private static final String REF = "fan_router";
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
  void emptyTheLab() throws SQLException {
    lab.cluster.reset();
    lab.router.reload();
  }

  @Test
  void routesKeysByTheDirectorysLiveRing() {
    ShardAddress section = lab.router.route("event-1/section-A");
    ShardAddress customer = lab.router.route("customer-42");

    // The same shards as the module's routing example, now with their machines from ring_shards.
    assertThat(section).isEqualTo(new ShardAddress(1, "shard-37", "shard-host-2"));
    assertThat(customer).isEqualTo(new ShardAddress(1, "shard-11", "shard-host-1"));
  }

  @Test
  void theDirectoryRefusesASecondLiveRingVersion() {
    assertThatThrownBy(() ->
            lab.runOnDirectory(
                "INSERT INTO directory.ring_versions VALUES (2, 1024, 'LIVE', now())"))
        .isInstanceOfSatisfying(
            SQLException.class,
            refused -> assertThat(Constraints.violated(refused)).contains("one_live_ring"));
  }

  @Test
  void aMachineRefusesAWriteForAShardItDoesNotOwn() throws Exception {
    ShardAddress customerShard = lab.router.route(ShardKeys.customer(42));
    lab.runAsOwner(
        customerShard.hostName(),
        "UPDATE routing.served_shards SET accepts_writes = false WHERE shard_name = ?",
        customerShard.shardName());

    Result<Long, WriteRefusal> written = lab.router.write(ShardKeys.customer(42), this::insertFan);

    assertThat(written).isEqualTo(Result.error(new WriteRefusal.NotOwner(customerShard)));
  }

  @Test
  void aPausedShardHoldsItsWritesUntilThePauseEnds() throws Exception {
    String shardName = lab.router.route(ShardKeys.customer(42)).shardName();
    try (ExecutorService writer = Executors.newVirtualThreadPerTaskExecutor()) {
      Future<Result<Long, WriteRefusal>> written;
      try (ShardPause _ = lab.router.pause(shardName)) {
        written = writer.submit(() -> lab.router.write(ShardKeys.customer(42), this::insertFan));
        // Still waiting while the pause lasts: nothing was written.
        await().during(Duration.ofMillis(300)).atMost(PATIENCE).until(() -> !written.isDone());
        assertThat(
                lab.countOn(
                    lab.router.route(ShardKeys.customer(42)), "SELECT count(*) FROM customers"))
            .isZero();
      }

      assertThat(written.get(PATIENCE.toSeconds(), TimeUnit.SECONDS))
          .isEqualTo(Result.success(42L));
    }
  }

  private Result<Long, WriteRefusal> insertFan(Connection connection) throws SQLException {
    try (PreparedStatement insert =
        connection.prepareStatement(
            "INSERT INTO customers (customer_id, customer_ref, created_at) VALUES (42, ?, now())")) {
      insert.setString(1, REF);
      insert.executeUpdate();
      return Result.success(42L);
    }
  }
}
