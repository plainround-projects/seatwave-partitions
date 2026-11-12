-- SeatWave's directory (Module 18): one small, unsharded database that every router reads at
-- start-up and on every reload, and that every id generator asks for its worker id.

-- One row per version of the ring. A router builds the ring from the shard list of the LIVE
-- version (HashRing.of), so every router that reads the same version routes every key the same.
CREATE TABLE ring_versions (
  ring_version INT PRIMARY KEY,
  virtual_nodes_per_shard INT NOT NULL,
  state VARCHAR(8) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT ring_points_in_range CHECK (virtual_nodes_per_shard BETWEEN 1 AND 4096),
  -- PLANNED: being built; MOVING: data copying, routers read both; LIVE: routing; RETIRED: old.
  CONSTRAINT known_ring_state CHECK (state IN ('PLANNED', 'MOVING', 'LIVE', 'RETIRED'))
);

-- At most one version routes writes at a time; the database refuses a second LIVE row.
CREATE UNIQUE INDEX one_live_ring ON ring_versions ((true)) WHERE state = 'LIVE';

-- Which shards a ring version has, and which machine (host) serves each shard. Moving a whole
-- shard to a new machine changes host_name only: the ring, and so every key's shard, stays.
CREATE TABLE ring_shards (
  ring_version INT NOT NULL REFERENCES ring_versions (ring_version),
  shard_name VARCHAR(32) NOT NULL,
  host_name VARCHAR(64) NOT NULL,
  CONSTRAINT ring_shards_pkey PRIMARY KEY (ring_version, shard_name),
  -- The same alphabet HashRing accepts, because the name is hashed into the ring's points.
  CONSTRAINT shard_name_format CHECK (shard_name ~ '^[a-z0-9-]{1,32}$')
);

-- Worker ids for the Snowflake-style generator. An instance takes a free id with a lease and
-- renews it; it stops issuing ids if it cannot renew before expires_at (WorkerLeases).
CREATE TABLE id_worker_leases (
  worker_id INT PRIMARY KEY,
  holder VARCHAR(64) NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT worker_id_fits_ten_bits CHECK (worker_id BETWEEN 0 AND 1023)
);
