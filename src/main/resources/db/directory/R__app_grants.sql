-- The application reads the ring and manages leases; it never changes the ring. Only the owner
-- (a migration, or the shard move run by an operator) writes ring_versions and ring_shards,
-- because whoever writes ring_shards decides where every shard's traffic goes (Section 10).
GRANT USAGE ON SCHEMA ${flyway:defaultSchema} TO ${appRole};
GRANT SELECT ON ring_versions, ring_shards TO ${appRole};
GRANT SELECT, INSERT, UPDATE, DELETE ON id_worker_leases TO ${appRole};
