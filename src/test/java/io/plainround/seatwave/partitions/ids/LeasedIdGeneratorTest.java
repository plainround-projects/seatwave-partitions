package io.plainround.seatwave.partitions.ids;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.plainround.seatwave.partitions.ids.IdGenerationRefusedException.Reason;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class LeasedIdGeneratorTest {

  private static final Instant START = Instant.parse("2026-11-12T12:00:00Z");
  private static final Duration TOLERANCE = Duration.ofMillis(10);
  private static final int WORKER = 7;

  @Test
  void issuesIdsWithTheLeasedWorkerIdBeforeTheLeaseRunsOut() {
    LeasedIdGenerator generator = generator(START.plusSeconds(1));

    long id = generator.nextId();

    assertThat(IdLayout.seatWave().workerOf(id)).isEqualTo(WORKER);
  }

  @Test
  void refusesFromTheInstantTheLeaseRunsOut() {
    LeasedIdGenerator generator = generator(START);

    assertThatThrownBy(generator::nextId)
        .isInstanceOfSatisfying(
            IdGenerationRefusedException.class,
            refused -> assertThat(refused.reason()).isEqualTo(Reason.WORKER_LEASE_EXPIRED));
  }

  @Test
  void issuesAgainAfterARenewalExtendsTheLease() {
    LeasedIdGenerator generator = generator(START);

    generator.renewed(new WorkerLease(WORKER, "instance-a", START.plusSeconds(1)));

    assertThat(generator.nextId()).isPositive();
  }

  @Test
  void rejectsARenewalForAnotherWorkerId() {
    LeasedIdGenerator generator = generator(START);

    assertThatIllegalArgumentException()
        .isThrownBy(() ->
            generator.renewed(new WorkerLease(WORKER + 1, "instance-a", START.plusSeconds(1))));
  }

  private static LeasedIdGenerator generator(Instant validUntil) {
    return new LeasedIdGenerator(
        IdLayout.seatWave(),
        new WorkerLease(WORKER, "instance-a", validUntil),
        new SteppedClock(START.toEpochMilli()),
        TOLERANCE);
  }
}
