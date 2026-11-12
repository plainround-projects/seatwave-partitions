package io.plainround.seatwave.partitions.ids;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.plainround.seatwave.partitions.ids.IdGenerationRefusedException.Reason;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/** Pins the invariant: ids are never equal, and each worker's ids always increase. */
class SnowflakeIdGeneratorTest {

  private static final long BASE_MILLIS = Instant.parse("2026-11-11T12:00:00Z").toEpochMilli();
  private static final Duration TOLERANCE = Duration.ofMillis(10);
  private static final int THREADS = 8;
  // 8 x 50,000 = 400,000 ids from eight threads at once on one generator.
  private static final int IDS_PER_THREAD = 50_000;

  @Test
  void issuesUniqueIdsThatIncreaseInEveryThreadWhenEightThreadsShareOneWorker() throws Exception {
    // The system clock, because the race needs real scheduling; the assertions do not depend on
    // what time it is.
    SnowflakeIdGenerator generator =
        new SnowflakeIdGenerator(IdLayout.seatWave(), 3, Clock.systemUTC(), TOLERANCE);
    List<Future<long[]>> results = new ArrayList<>();
    try (ExecutorService pool = Executors.newFixedThreadPool(THREADS)) {
      for (int threadIndex = 0; threadIndex < THREADS; threadIndex++) {
        results.add(pool.submit(() -> idsFrom(generator)));
      }
    }

    long[] everyId = new long[THREADS * IDS_PER_THREAD];
    int filled = 0;
    for (Future<long[]> result : results) {
      long[] idsOfThread = result.get();
      assertThat(idsOfThread).isSorted().doesNotHaveDuplicates();
      System.arraycopy(idsOfThread, 0, everyId, filled, idsOfThread.length);
      filled += idsOfThread.length;
    }
    assertThat(Arrays.stream(everyId).distinct().count()).isEqualTo(everyId.length);
  }

  @Test
  void waitsOutABackwardStepWithinToleranceAndStillIssuesALargerId() {
    // +0, +1, then 5 ms back; after the script the clock moves on 1 ms per read.
    SteppedClock clock = new SteppedClock(BASE_MILLIS, BASE_MILLIS + 1, BASE_MILLIS - 4);
    SnowflakeIdGenerator generator =
        new SnowflakeIdGenerator(IdLayout.seatWave(), 7, clock, TOLERANCE);
    long first = generator.nextId();
    long second = generator.nextId();

    long afterTheStep = generator.nextId();

    assertThat(afterTheStep).isGreaterThan(second).isGreaterThan(first);
    assertThat(generator.waitedMillis()).isEqualTo(5);
    assertThat(generator.refusals()).isZero();
  }

  @Test
  void refusesWhenTheClockStepsBackFurtherThanTheTolerance() {
    SteppedClock clock = new SteppedClock(BASE_MILLIS, BASE_MILLIS - 50);
    SnowflakeIdGenerator generator =
        new SnowflakeIdGenerator(IdLayout.seatWave(), 7, clock, TOLERANCE);
    generator.nextId();

    assertThatThrownBy(generator::nextId)
        .isInstanceOfSatisfying(
            IdGenerationRefusedException.class,
            refused -> {
              assertThat(refused.reason()).isEqualTo(Reason.CLOCK_BEHIND_BEYOND_TOLERANCE);
              assertThat(refused.clockBehindMillis()).isEqualTo(50);
            });
    assertThat(generator.refusals()).isEqualTo(1);
  }

  @Test
  void startsTheNextMillisecondWhenTheSequenceOfOneMillisecondRunsOut() {
    IdLayout layout = IdLayout.seatWave();
    int idsPerMillisecond = (int) layout.idsPerMillisecondPerWorker();
    // One reading more than a millisecond's worth of sequence numbers, all in the same millisecond.
    long[] readings = new long[idsPerMillisecond + 1];
    Arrays.fill(readings, BASE_MILLIS);
    SnowflakeIdGenerator generator =
        new SnowflakeIdGenerator(layout, 7, new SteppedClock(readings), TOLERANCE);
    long lastOfTheMillisecond = 0;
    for (int call = 0; call < idsPerMillisecond; call++) {
      lastOfTheMillisecond = generator.nextId();
    }

    long firstOfTheNext = generator.nextId();

    assertThat(layout.sequenceOf(lastOfTheMillisecond)).isEqualTo(layout.maxSequence());
    assertThat(layout.sequenceOf(firstOfTheNext)).isZero();
    assertThat(layout.timestampOf(firstOfTheNext))
        .isEqualTo(layout.timestampOf(lastOfTheMillisecond) + 1);
    assertThat(generator.waitedMillis()).isEqualTo(1);
  }

  @Test
  void rejectsAWorkerIdThatDoesNotFitTheLayout() {
    long tooLarge = IdLayout.seatWave().maxWorkerId() + 1;

    assertThatIllegalArgumentException()
        .isThrownBy(() ->
            new SnowflakeIdGenerator(IdLayout.seatWave(), tooLarge, Clock.systemUTC(), TOLERANCE))
        .withMessageContaining("worker id must be 0 to 1023");
  }

  private static long[] idsFrom(SnowflakeIdGenerator generator) {
    long[] ids = new long[IDS_PER_THREAD];
    for (int index = 0; index < IDS_PER_THREAD; index++) {
      ids[index] = generator.nextId();
    }
    return ids;
  }
}
