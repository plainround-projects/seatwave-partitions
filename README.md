# seatwave-partitions

SeatWave's booking data split over 64 shards: a consistent-hash ring with virtual nodes, a comparison of five shard keys, a Snowflake-style ID generator with a worker-id lease, a router that reads the ring from a directory database, the reserve-then-claim hold flow across shards, and a measured move of one shard between two PostgreSQL machines by logical replication while it takes writes. It is the project of Module 18 ("Partitioning and IDs") of the course System Design Fundamentals.

## What it shows

- `ring`: 10,000,000 keys on rings of 3 and 64 shards with 1 to 1,024 points each; skew; keys moved when a shard is added, against hash mod N (`./gradlew ringSimulation`).
- `shardkey`: rows and peak writes per shard for five candidate keys at SeatWave's planned volume, against one shard's capacity, 22,316 writes a second as Module 14 measured it (`./gradlew shardKeys`).
- `ids`: the 41/10/12 layout from 2026-01-01, 4,000,000 IDs from 16 threads, a clock stepping back 5 ms (waited out) and 50 ms (refused) (`./gradlew idGenerator`); the worker-id lease on `id_worker_leases` and a generator that stops when its lease runs out.
- `routing`, `holds`, `lab`: the directory (`ring_versions`, `ring_shards`), two shard machines with 32 shard schemas each, a router that refuses nothing itself but is refused by any machine that does not own the shard (`write_to_owner_only`), and holds that reserve on the customer's shard, route the id, then claim the seat on the seat's shard.
- `move`: the shard move (`./gradlew shardMove`), measured.

## Prerequisites

- Eclipse Temurin 25 (any 25.x; the outputs below are from 25.0.2, standing in for the current 25.0.5).
- Docker with Compose v2 for the stack (the outputs below: Docker 24.0.6, Compose v2.23.0, Docker Desktop on an Apple-silicon Mac, a Linux virtual machine of 12 CPUs and 8 GB). `./gradlew check` needs no Docker.
- The wrapper downloads Gradle 9.9.1 and checks its SHA-256.

## Dependencies

Every version checked on 12 November 2026 on Maven Central (newest release) and OSV (no vulnerability listed).

| Coordinates | License | Why the JDK alone is not enough |
|---|---|---|
| `com.github.sviperll:result4j:1.2.0` | Apache-2.0 | `Result` values for expected refusals (code guidelines, rule 47) |
| `com.zaxxer:HikariCP:7.1.0` | Apache-2.0 | one bounded connection pool per machine, shared by every request |
| `org.postgresql:postgresql:42.7.14` | BSD-2-Clause | the JDBC driver; the router reads the constraint name of a refusal from it |
| `org.flywaydb:flyway-core:13.12.1`, `flyway-database-postgresql:13.12.1` | Apache-2.0 | versioned migrations for the directory, each machine and each of 64 shard schemas |
| `org.slf4j:slf4j-api:2.0.20`, `ch.qos.logback:logback-classic:1.6.6` | MIT; EPL-1.0 or LGPL-2.1 | the logging HikariCP and Flyway use, kept at WARN |
| `org.junit:junit-bom:6.1.3` | EPL-2.0 | the test runner |
| `org.assertj:assertj-core:3.27.7` | Apache-2.0 | assertions |
| `org.awaitility:awaitility:4.3.0` | Apache-2.0 | waits for "the fence waits for a lock" without a guessed sleep |
| Gradle plugin `com.diffplug.spotless:8.11.0` with google-java-format 2.0.0; JaCoCo 0.8.16 | Apache-2.0; EPL-2.0 | the formatter and the 0.80 line-coverage floor, both in `check` |
| Image `postgres:18.6@sha256:e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42` | PostgreSQL License | the directory and the two shard machines; index digest unchanged since 6 November 2026, arm64 variant present |

The ring, the shard-key comparison and the generator use no library: `MessageDigest` (MD5), `TreeMap`, `java.time.Clock` and `java.util.concurrent`.

## Commands and what they printed

All on 12 November 2026, on the verification machine (Apple-silicon Mac, Temurin 25.0.2; the stack in Docker Desktop's virtual machine, so every request crossed into it).

**Build and unit tests** (`./gradlew clean check`: 36 tests, formatter, line coverage 88.2 percent of the code that runs without a database, floor 0.80):

```text
$ ./gradlew clean check
> Task :spotlessJava
> Task :spotlessJavaCheck
> Task :spotlessCheck
> Task :check
> Task :jacocoTestReport
BUILD SUCCESSFUL in 2s
9 actionable tasks: 9 executed
Consider enabling configuration cache to speed up this build: https://docs.gradle.org/9.9.1/userguide/configuration_cache_enabling.html
```

**The three JDK-only programs** (each run twice, identical): `./gradlew ringSimulation`, `./gradlew shardKeys`, `./gradlew idGenerator`. The shard-key comparison:

```text
Candidate shard keys on 64 shards (consistent hashing, 1024 virtual nodes per shard), seed 18

ROWS AT 10,000,000,000 BOOKINGS (sample of 2,000,000, scaled 5,000 times; 2,000 bytes a booking)
  key                          average/shard   largest shard   skew   largest (GB)
  event_id                       156,250,000     175,225,000   1.12          350.5
  customer_id                    156,250,000     209,610,000   1.34          419.2
  booking_id                     156,250,000     171,660,000   1.10          343.3
  (event_id, section_code)       156,250,000     170,620,000   1.09          341.2
  booking month (range)          156,250,000     158,195,000   1.01          316.4

Per-shard write capacity: 22,316/s measured (Module 14), safe rate at 70%: 15,621/s

PEAK WRITES, TODAY'S PEAK, even demand: 13,000 writes in one second on event 1
  key                        shards used   hottest/s   average/s    skew  hottest shard against its capacity
  event_id                             1      13,000         203    64.0  fits (58% of capacity)
  customer_id                         64         258         203     1.3  fits (1% of capacity)
  booking_id                          64         230         203     1.1  fits (1% of capacity)
  (event_id, section_code)            18       1,305         203     6.4  fits (6% of capacity)
  booking month (range)                1      13,000         203    64.0  fits (58% of capacity)

PEAK WRITES, NATIONAL TOUR UNMETERED, even demand: 66,665 writes in one second on event 1
  key                        shards used   hottest/s   average/s    skew  hottest shard against its capacity
  event_id                             1      66,665       1,042    64.0  OVER CAPACITY (299% of capacity)
  customer_id                         64       1,177       1,042     1.1  fits (5% of capacity)
  booking_id                          64       1,123       1,042     1.1  fits (5% of capacity)
  (event_id, section_code)            18       6,631       1,042     6.4  fits (30% of capacity)
  booking month (range)                1      66,665       1,042    64.0  OVER CAPACITY (299% of capacity)

PEAK WRITES, NATIONAL TOUR UNMETERED, sections A-D take 40%: 66,665 writes in one second on event 1
  key                        shards used   hottest/s   average/s    skew  hottest shard against its capacity
  event_id                             1      66,665       1,042    64.0  OVER CAPACITY (299% of capacity)
  customer_id                         64       1,183       1,042     1.1  fits (5% of capacity)
  booking_id                          64       1,159       1,042     1.1  fits (5% of capacity)
  (event_id, section_code)            18       6,847       1,042     6.6  fits (31% of capacity)
  booking month (range)                1      66,665       1,042    64.0  OVER CAPACITY (299% of capacity)

RULES THAT STAY ON ONE SHARD (Module 13)
  key                        one_claim_per_seat     four holds per customer 
  event_id                   yes                    no                      
  customer_id                no                     yes                     
  booking_id                 no                     no                      
  (event_id, section_code)   yes                    no                      
  booking month (range)      no                     no                      
```

**The stack tests** (`scripts/stack.sh test --run --yes`: up, 18 tests, the combined coverage check, down -v; line coverage of all main code but the programs over both runs 90.4 percent):

```text
$ scripts/stack.sh test --run --yes
HoldFlowStackTest > aConfirmOfAnUnknownHoldIsRefused() PASSED
HoldFlowStackTest > aConfirmedBookingIsListedInMyTicketsAndTheHoldNoLongerCounts() PASSED
HoldFlowStackTest > aHoldForAnUnknownCustomerIsRefusedOnTheCustomersShard() PASSED
HoldFlowStackTest > aHoldIsFoundByItsIdAloneThroughItsRoute() PASSED
HoldFlowStackTest > oneSeatGoesToOneFanAndTheLosersReservationsAreGivenBack() PASSED
HoldFlowStackTest > aCustomerGetsAtMostFourHoldsWhenFiftyArriveAtOnceOnManyShards() PASSED
ShardMoveStackTest > theFenceWaitsForAWriteInFlightSoTheWriteIsCopiedBeforeTheSwitch() PASSED
ShardMoveStackTest > aStaleRoutersWriteToTheOldMachineIsRefusedAfterTheSwitch() PASSED
ShardMoveStackTest > aShardMovesUnderWritesWithNoWriteLostOrDoubled() PASSED
ShardRouterStackTest > aMachineRefusesAWriteForAShardItDoesNotOwn() PASSED
ShardRouterStackTest > aPausedShardHoldsItsWritesUntilThePauseEnds() PASSED
ShardRouterStackTest > theDirectoryRefusesASecondLiveRingVersion() PASSED
ShardRouterStackTest > routesKeysByTheDirectorysLiveRing() PASSED
WorkerLeaseStackTest > aReleasedIdIsFreeAtOnce() PASSED
WorkerLeaseStackTest > anExpiredLeaseGoesToTheNextHolderAndTheOldHolderCannotRenewIt() PASSED
WorkerLeaseStackTest > aLiveLeaseIsNotGivenToASecondHolder() PASSED
WorkerLeaseStackTest > racingInstancesNeverShareAWorkerId() PASSED
WorkerLeaseStackTest > aRenewalKeepsTheIdAndEndsLaterThanTheGrant() PASSED
BUILD SUCCESSFUL in 30s
nothing of seatwave-partitions is left
```

Each concurrency and ownership rule was mutation-checked: the rule was removed in a copy, the stack tests run on a fresh stack, and the test that guards it failed (lease granted without the expiry check; reservation trigger without its row lock; fence without its table lock; ownership trigger always passing). The captured runs are in the course's tools folder (`stack-mutants.txt`).

**The shard move** (`scripts/stack.sh up --run`, then `./gradlew shardMove`, under the course's measurement lock, load average 4.95 4.35 4.18 before the run):

```text
$ ./gradlew -q shardMove
Shard move of shard-17 from shard-host-1 to shard-host-2 (ring version 1, 64 shards, 1,024 points each)
SEED   2,500 sections on shard-17: 2,500,000 seats, 2,250,000 holds and 2,250,000 bookings in 56.5 s
       its tables on shard-host-1: 623 MB without indexes, 1,432 MB with indexes
WRITER 10,000 customers registered in 10.9 s (worker id 0); 200 holds a second, each confirmed, on the free seats of shard-17
MOVE   copy:     23.68 s, 26.3 MB/s of table data
       catch-up: 84 ms (a marker written after the copy, until visible on shard-host-2)
       pause:    36.0 ms (fence 4.5 ms, lag to 0 0.4 ms, directory flip and reload 31.2 ms)
       the copy on shard-host-2 with its indexes: 1,435 MB
WRITES sent 5,993; holds acknowledged 5,993; bookings acknowledged 5,993; refusals none; failures 0
AUDIT  lost 0, doubled 0
       holds on the new machine 2,255,993 = seed 2,250,000 + acknowledged 5,993
       bookings on the new machine 2,255,993 = seed 2,250,000 + acknowledged 5,993
       acknowledged ids found there: 5,993 of 5,993 holds, 5,993 of 5,993 bookings
       holds left on the old machine 2,255,378; writer holds there but not on the new one: 0
STALE  a router that read the directory before the move sends a hold on event-3/section-K K-1000 to shard-host-1:
       Error[error=NotOwner[address=ShardAddress[ringVersion=1, shardName=shard-17, hostName=shard-host-1]]]
       after its reload it sends the same hold to shard-host-2: accepted
FULL SIZE, the module's 312.5 GB shard (table data) at this run's rate:
       26.3 MB/s is 0.11 times the assumed 250 MB/s; copy 198 minutes (assumed 21)
       pause 0.036 s against the assumed 0.6 s
```

How to read it: the copy moved 623 MB of table data (1,432 MB with indexes, which the new machine built during the copy) in 23.68 s, 26.3 MB a second, with PostgreSQL's default two table-sync workers into a 4-CPU, 1.5 GB container; which resource bounded the rate was not established. The pause at the switch was 36 ms, most of it the router's reload of the directory and the rebuild of its 65,536-point ring. The data scale: 2,250,000 bookings, 1/69.4 of a shard's 156,250,000 at the lifetime volume. A laptop number, not production's.

**Stop** (`scripts/stack.sh down --run --yes`) prints `nothing of seatwave-partitions is left` after checking containers, networks and volumes by the project's label.

## Ports

127.0.0.1 only: 21800 the directory, 21801 `shard-host-1`, 21802 `shard-host-2`.

## Project structure

```text
compose.yaml                     the directory and two shard machines, pinned by digest
config/stack.properties          every setting of the stack tests and the move
db/init/10-roles.sh              owner, application and replicator roles, from Compose secrets
scripts/stack.sh                 up, down, test, status; dry run by default
src/main/resources/db/directory  ring_versions, one_live_ring, ring_shards, id_worker_leases
src/main/resources/db/host       served_shards and the ownership check, once per machine
src/main/resources/db/shard      Module 13's V1 to V3 unchanged, V4 for a shard, grants
src/main/java/.../partitions     ring, shardkey, ids, config, routing, holds, lab, move
src/test/java/.../partitions     unit tests; stack/ for the tests tagged "stack"
```

## What is simplified

- No dual reads (Section 6e, step 3) and no reverse stream for rollback; the old copy stays, refusing writes.
- A machine checks that it owns a shard, not that a key belongs to the shard; a ring change (65 shards) would need that per-key check.
- `customer_ref` is unique per shard only; production code would keep a global lookup by ref.
- "My tickets" and the booking's route are written after the booking without an outbox (Module 28).
- The move runs its publication and subscription as the superuser; production would use a role made for moves.
- The subscription's connection string holds the lab's generated replicator password, readable by the target's superuser.

## No CI on purpose

There is no `.github/` folder: the course runs every command on the verification machine and prints the output here.

## The Compose path

`scripts/stack.sh up --run` writes four random passwords into `.secrets/` (ignored by git), starts the three databases with `docker compose -p seatwave-partitions up -d --wait` and waits for their health checks; `scripts/stack.sh down --run` removes them with `down -v` and deletes the passwords.

## License

MIT, see `LICENSE`.
