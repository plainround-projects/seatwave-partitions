package io.plainround.seatwave.partitions.stack;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sviperll.result4j.Result;
import io.plainround.seatwave.partitions.holds.HoldFlow;
import io.plainround.seatwave.partitions.holds.HoldView;
import io.plainround.seatwave.partitions.lab.SectionKey;
import io.plainround.seatwave.partitions.routing.ShardAddress;
import io.plainround.seatwave.partitions.routing.ShardKeys;
import io.plainround.seatwave.partitions.routing.WriteRefusal;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("stack")
class HoldFlowStackTest {

  private static final long EVENT = 1;
  private static final int SECTIONS = 20;
  // Module 13's limit race: 50 holds for one customer at once, on 50 different seats.
  private static final int LIMIT_RACERS = 50;
  private static final int HOLD_LIMIT = 4;
  private static final int SEAT_RACERS = 20;
  private static final int FANS = SEAT_RACERS;
  private static final Duration PATIENCE = Duration.ofSeconds(30);
  private static StackLab lab;
  private static List<SectionKey> sections;
  private List<Long> fans;
  private HoldFlow flow;

  @BeforeAll
  static void openLab() throws IOException, SQLException {
    lab = StackLab.open();
    sections =
        IntStream.range(0, SECTIONS)
            .mapToObj(index -> new SectionKey(EVENT, String.valueOf((char) ('A' + index))))
            .toList();
  }

  @AfterAll
  static void closeLab() {
    lab.close();
  }

  @BeforeEach
  void startFromEventOneAndFreshFans() throws Exception {
    lab.cluster.reset();
    lab.router.reload();
    lab.cluster.writeSections(lab.router, sections);
    fans = lab.registerCustomers("hold-flow-test", FANS);
    flow = new HoldFlow(lab.router, lab.settings.holdDuration());
  }

  @Test
  void aCustomerGetsAtMostFourHoldsWhenFiftyArriveAtOnceOnManyShards() throws Exception {
    long fan = fans.getFirst();
    List<String> seats =
        IntStream.range(0, LIMIT_RACERS)
            .mapToObj(index -> sections.get(index % SECTIONS).seatLabel(1 + index / SECTIONS))
            .toList();

    List<Result<UUID, WriteRefusal>> results = race(seats, seat -> flow.hold(fan, EVENT, seat));

    assertThat(results).filteredOn(result -> !result.isError()).hasSize(HOLD_LIMIT);
    assertThat(results)
        .filteredOn(result -> result.isError())
        .containsOnly(Result.error(new WriteRefusal.HoldLimitReached(fan)));
    assertThat(reservationsOf(fan)).isEqualTo(HOLD_LIMIT);
  }

  @Test
  void oneSeatGoesToOneFanAndTheLosersReservationsAreGivenBack() throws Exception {
    String seat = sections.getFirst().seatLabel(1);
    List<Long> racers = fans.subList(0, SEAT_RACERS);

    List<Result<UUID, WriteRefusal>> results = raceFans(racers, fan -> flow.hold(fan, EVENT, seat));

    assertThat(results).filteredOn(result -> !result.isError()).hasSize(1);
    assertThat(results)
        .filteredOn(result -> result.isError())
        .containsOnly(Result.error(new WriteRefusal.SeatTaken(EVENT, seat)));
    long reservations = 0;
    for (long fan : racers) {
      reservations += reservationsOf(fan);
    }
    assertThat(reservations).isEqualTo(1);
  }

  @Test
  void aHoldIsFoundByItsIdAloneThroughItsRoute() throws Exception {
    long fan = fans.getFirst();
    String seat = sections.get(2).seatLabel(7);
    UUID holdId = flow.hold(fan, EVENT, seat).discardError().orElseThrow();

    Result<HoldView, WriteRefusal> found = flow.findHold(holdId);

    assertThat(found).isEqualTo(Result.success(new HoldView(holdId, EVENT, seat, fan, "ACTIVE")));
  }

  @Test
  void aConfirmedBookingIsListedInMyTicketsAndTheHoldNoLongerCounts() throws Exception {
    long fan = fans.getFirst();
    UUID holdId = flow.hold(fan, EVENT, sections.get(3).seatLabel(9)).discardError().orElseThrow();

    UUID bookingId = flow.confirm(holdId).discardError().orElseThrow();

    assertThat(flow.myTickets(fan)).containsExactly(bookingId);
    assertThat(reservationsOf(fan)).isZero();
    ShardAddress routeShard = lab.router.route(ShardKeys.route(bookingId));
    assertThat(
            lab.countOn(
                routeShard, "SELECT count(*) FROM id_routes WHERE entity_id = ?", bookingId))
        .isEqualTo(1);
  }

  @Test
  void aHoldForAnUnknownCustomerIsRefusedOnTheCustomersShard() throws Exception {
    long stranger = 1;

    Result<UUID, WriteRefusal> refused =
        flow.hold(stranger, EVENT, sections.getFirst().seatLabel(2));

    assertThat(refused).isEqualTo(Result.error(new WriteRefusal.UnknownCustomer(stranger)));
  }

  @Test
  void aConfirmOfAnUnknownHoldIsRefused() throws Exception {
    UUID nobody = UUID.fromString("019a7b2e-0000-7000-8000-000000000001");

    assertThat(flow.confirm(nobody)).isEqualTo(Result.error(new WriteRefusal.UnknownHold(nobody)));
  }

  private static long reservationsOf(long fan) throws SQLException {
    return lab.countOn(
        lab.router.route(ShardKeys.customer(fan)),
        "SELECT count(*) FROM hold_reservations WHERE customer_id = ?",
        fan);
  }

  /** One attempt of a race. */
  @FunctionalInterface
  private interface Attempt<T> {
    Result<UUID, WriteRefusal> run(T input) throws Exception;
  }

  private static List<Result<UUID, WriteRefusal>> race(List<String> inputs, Attempt<String> attempt)
      throws Exception {
    return raceAll(inputs, attempt);
  }

  private static List<Result<UUID, WriteRefusal>> raceFans(List<Long> inputs, Attempt<Long> attempt)
      throws Exception {
    return raceAll(inputs, attempt);
  }

  // Every racer waits at one barrier, then all go at once, so the database sees them together.
  private static <T> List<Result<UUID, WriteRefusal>> raceAll(List<T> inputs, Attempt<T> attempt)
      throws Exception {
    CyclicBarrier start = new CyclicBarrier(inputs.size());
    List<Future<Result<UUID, WriteRefusal>>> futures = new ArrayList<>();
    try (ExecutorService racers = Executors.newVirtualThreadPerTaskExecutor()) {
      for (T input : inputs) {
        futures.add(
            racers.submit(() -> {
              start.await(PATIENCE.toSeconds(), TimeUnit.SECONDS);
              return attempt.run(input);
            }));
      }
    }
    List<Result<UUID, WriteRefusal>> results = new ArrayList<>();
    for (Future<Result<UUID, WriteRefusal>> future : futures) {
      results.add(future.get());
    }
    return results;
  }
}
