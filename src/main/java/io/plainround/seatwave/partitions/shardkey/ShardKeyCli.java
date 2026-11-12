package io.plainround.seatwave.partitions.shardkey;

import io.plainround.seatwave.partitions.ring.HashRing;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

/**
 * Compares five shard keys for SeatWave's booking data on the planned ring (64 shards, 1,024
 * virtual nodes each): rows per shard at 10,000,000,000 bookings, writes per shard in one peak
 * second today and at the national tour's rate, and which rules stay on one shard. A seeded random
 * generator makes every run print the same numbers. Run with {@code ./gradlew shardKeys}.
 */
public final class ShardKeyCli {

  private static final long SEED = 18L;
  private static final int SHARD_COUNT = 64;
  private static final int VIRTUAL_NODES = 1_024; // the plan, from RingSimulationCli's Part 3

  // The lifetime mix (Section 7): 10,000,000,000 booking slots in three kinds of event.
  private static final EventKind ARENA = new EventKind(250_000, 20, 1_000, 1);
  private static final EventKind THEATRE = new EventKind(2_250_000, 5, 400, 250_001);
  private static final EventKind CLUB = new EventKind(2_500_000, 1, 200, 2_500_001);
  // A sample of 1 in 5,000 bookings; each shard then gets about 31,000 samples, enough for a
  // count within about 1 percent of its true share.
  private static final int SAMPLE_SIZE = 2_000_000;
  private static final long BYTES_PER_BOOKING = 2_000L; // Module 3, row with indexes and history
  private static final long LIFETIME_FANS = 100_000_000L;
  // One box-office account that books 0.5 percent of everything: the "whale" a customer key meets.
  private static final long BOX_OFFICE_CUSTOMER = 1L;
  private static final double BOX_OFFICE_SHARE = 0.005;

  // Peak writes: Module 14's 13,000 hold inserts a second, all on event 1 (Arena Night, Module 13).
  private static final int TODAY_PEAK_WRITES = 13_000;
  // Module 3's national tour, 1,000,000 fans, at Module 1's 13,333 hold attempts per 200,000 fans.
  private static final int TOUR_PEAK_WRITES = 13_333 * (1_000_000 / 200_000);
  private static final int ON_SALE_FANS = 200_000;
  private static final long HOT_EVENT = 1L;
  private static final int ARENA_SECTIONS = 20;
  // Front-loaded demand: sections A to D take 10 percent each; the other 16 share 60 percent.
  private static final int FRONT_SECTIONS = 4;
  private static final double FRONT_SECTION_SHARE = 0.10;

  // Per-shard capacity: Module 14's measured hold inserts a second with its final index set (on a
  // laptop at a quarter of the study volume, bounded by the round trip into Docker's virtual
  // machine; estimated there at 22,857), and Module 3's 70 percent utilization target.
  private static final double SHARD_CAPACITY_WRITES = 22_316;
  private static final double UTILIZATION_TARGET = 0.70;
  private static final double BYTES_PER_GIGABYTE = 1e9;
  private static final double PERCENT = 100.0;

  private ShardKeyCli() {}

  /** Prints the three comparisons. */
  public static void main(String[] arguments) {
    List<String> shardNames =
        IntStream.range(0, SHARD_COUNT)
            .mapToObj(shardNumber -> String.format(Locale.ROOT, "shard-%02d", shardNumber))
            .toList();
    HashRing ring = HashRing.of(shardNames, VIRTUAL_NODES);
    System.out.printf(
        Locale.ROOT,
        "Candidate shard keys on %d shards (consistent hashing, %d virtual nodes per shard), seed"
            + " %d%n",
        SHARD_COUNT,
        VIRTUAL_NODES,
        SEED);
    printLifetimeRows(ring);
    double safeRate = SHARD_CAPACITY_WRITES * UTILIZATION_TARGET;
    System.out.printf(
        Locale.ROOT,
        "%nPer-shard write capacity: %,.0f/s measured (Module 14), safe rate at 70%%: %,.0f/s%n",
        SHARD_CAPACITY_WRITES,
        safeRate);
    printPeak(ring, "TODAY'S PEAK, even demand", TODAY_PEAK_WRITES, false);
    printPeak(ring, "NATIONAL TOUR UNMETERED, even demand", TOUR_PEAK_WRITES, false);
    printPeak(ring, "NATIONAL TOUR UNMETERED, sections A-D take 40%", TOUR_PEAK_WRITES, true);
    printRules();
  }

  private static void printLifetimeRows(HashRing ring) {
    SplittableRandom random = new SplittableRandom(SEED);
    long lifetimeBookings = ARENA.bookings() + THEATRE.bookings() + CLUB.bookings();
    Map<CandidateKey, Map<String, Long>> samplesPerShard = new HashMap<>();
    for (int sampleIndex = 0; sampleIndex < SAMPLE_SIZE; sampleIndex++) {
      BookingWrite write = sampledBooking(random, lifetimeBookings);
      for (CandidateKey key : CandidateKey.values()) {
        samplesPerShard
            .computeIfAbsent(key, ignored -> new HashMap<>())
            .merge(key.shardOf(write, ring), 1L, Long::sum);
      }
    }
    double scale = (double) lifetimeBookings / SAMPLE_SIZE;
    double averageRows = (double) lifetimeBookings / SHARD_COUNT;
    System.out.printf(
        Locale.ROOT,
        "%nROWS AT %,d BOOKINGS (sample of %,d, scaled %,.0f times; 2,000 bytes a booking)%n",
        lifetimeBookings,
        SAMPLE_SIZE,
        scale);
    System.out.printf(
        Locale.ROOT,
        "  %-26s %15s %15s %6s %14s%n",
        "key",
        "average/shard",
        "largest shard",
        "skew",
        "largest (GB)");
    for (CandidateKey key : CandidateKey.values()) {
      long largestSample =
          samplesPerShard.get(key).values().stream().mapToLong(Long::longValue).max().orElse(0L);
      double largestRows = largestSample * scale;
      System.out.printf(
          Locale.ROOT,
          "  %-26s %,15.0f %,15.0f %6.2f %,14.1f%n",
          key.label(),
          averageRows,
          largestRows,
          largestRows / averageRows,
          largestRows * BYTES_PER_BOOKING / BYTES_PER_GIGABYTE);
    }
  }

  private static BookingWrite sampledBooking(SplittableRandom random, long lifetimeBookings) {
    // Every booking slot of every event is equally likely, so big events are drawn in proportion
    // to their size, exactly as their bookings would be.
    long slot = random.nextLong(lifetimeBookings);
    EventKind kind = ARENA;
    if (slot >= ARENA.bookings()) {
      slot -= ARENA.bookings();
      kind = THEATRE;
      if (slot >= THEATRE.bookings()) {
        slot -= THEATRE.bookings();
        kind = CLUB;
      }
    }
    long seatsPerEvent = (long) kind.sections() * kind.seatsPerSection();
    long eventId = kind.firstEventId() + slot / seatsPerEvent;
    int sectionIndex = (int) (slot % seatsPerEvent / kind.seatsPerSection());
    long customerId =
        random.nextDouble() < BOX_OFFICE_SHARE
            ? BOX_OFFICE_CUSTOMER
            : BOX_OFFICE_CUSTOMER + 1 + random.nextLong(LIFETIME_FANS);
    return new BookingWrite(
        eventId,
        sectionCode(sectionIndex),
        customerId,
        random.nextLong(),
        random.nextInt(CandidateKey.PLANNED_MONTHS));
  }

  private static void printPeak(HashRing ring, String title, int writes, boolean frontLoaded) {
    SplittableRandom random = new SplittableRandom(SEED);
    Map<CandidateKey, Map<String, Long>> writesPerShard = new HashMap<>();
    for (int writeIndex = 0; writeIndex < writes; writeIndex++) {
      BookingWrite write =
          new BookingWrite(
              HOT_EVENT,
              sectionCode(peakSection(random, frontLoaded)),
              BOX_OFFICE_CUSTOMER + 1 + random.nextInt(ON_SALE_FANS),
              random.nextLong(),
              CandidateKey.PLANNED_MONTHS - 1);
      for (CandidateKey key : CandidateKey.values()) {
        writesPerShard
            .computeIfAbsent(key, ignored -> new HashMap<>())
            .merge(key.shardOf(write, ring), 1L, Long::sum);
      }
    }
    double averageWrites = (double) writes / SHARD_COUNT;
    double safeRate = SHARD_CAPACITY_WRITES * UTILIZATION_TARGET;
    System.out.printf(
        Locale.ROOT,
        "%nPEAK WRITES, %s: %,d writes in one second on event %d%n",
        title,
        writes,
        HOT_EVENT);
    System.out.printf(
        Locale.ROOT,
        "  %-26s %11s %11s %11s %7s  %s%n",
        "key",
        "shards used",
        "hottest/s",
        "average/s",
        "skew",
        "hottest shard against its capacity");
    for (CandidateKey key : CandidateKey.values()) {
      Map<String, Long> perShard = writesPerShard.get(key);
      long hottest = perShard.values().stream().mapToLong(Long::longValue).max().orElse(0L);
      String verdict =
          hottest > SHARD_CAPACITY_WRITES
              ? "OVER CAPACITY"
              : hottest > safeRate ? "over the safe rate" : "fits";
      System.out.printf(
          Locale.ROOT,
          "  %-26s %11d %,11d %,11.0f %7.1f  %s (%.0f%% of capacity)%n",
          key.label(),
          perShard.size(),
          hottest,
          averageWrites,
          hottest / averageWrites,
          verdict,
          PERCENT * hottest / SHARD_CAPACITY_WRITES);
    }
  }

  private static int peakSection(SplittableRandom random, boolean frontLoaded) {
    if (!frontLoaded) {
      return random.nextInt(ARENA_SECTIONS);
    }
    double draw = random.nextDouble();
    double frontShare = FRONT_SECTIONS * FRONT_SECTION_SHARE;
    if (draw < frontShare) {
      return (int) (draw / FRONT_SECTION_SHARE);
    }
    return FRONT_SECTIONS + random.nextInt(ARENA_SECTIONS - FRONT_SECTIONS);
  }

  private static void printRules() {
    System.out.printf(Locale.ROOT, "%nRULES THAT STAY ON ONE SHARD (Module 13)%n");
    System.out.printf(
        Locale.ROOT,
        "  %-26s %-22s %-24s%n",
        "key",
        "one_claim_per_seat",
        "four holds per customer");
    for (CandidateKey key : CandidateKey.values()) {
      System.out.printf(
          Locale.ROOT,
          "  %-26s %-22s %-24s%n",
          key.label(),
          CandidateKey.yesOrNo(key.keepsSeatRuleOnOneShard()),
          CandidateKey.yesOrNo(key.keepsHoldLimitOnOneShard()));
    }
  }

  private static String sectionCode(int sectionIndex) {
    // Sections are lettered from A, as in Module 13's seed (A to T for the arena).
    return String.valueOf((char) ('A' + sectionIndex));
  }

  /**
   * One kind of event in the lifetime mix.
   *
   * @param events how many events of this kind
   * @param sections sections per event
   * @param seatsPerSection seats per section, all booked over the event's life
   * @param firstEventId the first event id of this kind; kinds use consecutive id ranges
   */
  private record EventKind(long events, int sections, int seatsPerSection, long firstEventId) {
    long bookings() {
      return events * sections * seatsPerSection;
    }
  }
}
