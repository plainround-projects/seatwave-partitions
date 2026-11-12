# Changelog

All notable changes to this project are written here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-11-12

The first build of Module 18's code seed (16 files), run for the first time with Gradle and against
PostgreSQL 18.6.

### Added

- The Gradle wrapper (9.9.1, both checksums compared), version catalog, lockfiles, Spotless, the
  JaCoCo floor, README, ARCHITECTURE, this changelog and the license.
- `compose.yaml`: the directory and two shard machines on ports 21800 to 21802, `scripts/stack.sh`.
- `db/shard-directory.sql` split into Flyway migrations: `db/directory` (the ring and the leases),
  `db/host` (new: `served_shards` and the ownership trigger) and `db/shard` (Module 13's V1 to V3
  unchanged, and V4 with `id_routes`, `hold_reservations`, `bookings_by_customer`, the new
  `move_markers`, `uuidv7()` defaults).
- `ShardRouter`, `WorkerLeases` and `LeasedIdGenerator`, `HoldFlow` and `Customers`, `ShardMover`
  and `ShardMoveCli` (`./gradlew shardMove`), and 18 stack tests; unit tests for the settings, the
  leased generator, the candidate keys, the key distribution and the value records.

### Changed

- `ShardKeyCli` uses Module 14's measured 22,316 writes a second per shard (was the estimate
  22,857); the safe rate is now 15,621 (was 16,000).
- `IdGenerationRefusedException.Reason` has a new value, `WORKER_LEASE_EXPIRED`.
- On a shard, Module 13's `hold_by_a_real_customer` foreign key and its `hold_limit_per_customer`
  trigger are dropped (the rule moves to `reservation_limit_per_customer`), and `customer_id` is no
  longer an identity column: ids come from the leased Snowflake-style generator.
