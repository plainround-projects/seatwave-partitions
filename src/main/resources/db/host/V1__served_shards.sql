-- On every shard machine (Module 18): which logical shards this machine serves, and whether it
-- accepts writes for each. Every shard table has a statement trigger that calls
-- refuse_unless_owner(), so a router with a stale ring that sends a write here for a shard this
-- machine no longer owns gets an error instead of splitting the shard's data over two machines.

CREATE TABLE served_shards (
  shard_name VARCHAR(32) PRIMARY KEY,
  -- shard-17 lives in the schema shard_17: a schema name cannot hold a hyphen unquoted.
  schema_name VARCHAR(32) NOT NULL,
  -- false while a copy is arriving (the target of a move) and after the switch (the old source).
  accepts_writes BOOLEAN NOT NULL,
  CONSTRAINT one_shard_per_schema UNIQUE (schema_name),
  CONSTRAINT served_shard_name_format CHECK (shard_name ~ '^[a-z0-9-]{1,32}$'),
  CONSTRAINT schema_matches_shard CHECK (schema_name = replace(shard_name, '-', '_'))
);

-- A plain SELECT, no row lock: it runs in a VOLATILE function, so at read committed it takes a
-- fresh snapshot and sees a fence the moment the fence commits. The fence (ShardMover) locks the
-- shard's tables in SHARE mode before it flips accepts_writes, so it waits for every write that
-- passed this check and has not committed yet, and blocks new ones until the flip is visible.
CREATE FUNCTION refuse_unless_owner() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM ${flyway:defaultSchema}.served_shards
      WHERE schema_name = TG_TABLE_SCHEMA AND accepts_writes) THEN
    RAISE EXCEPTION 'this machine does not accept writes for the shard in schema %', TG_TABLE_SCHEMA
      USING ERRCODE = 'object_not_in_prerequisite_state', CONSTRAINT = 'write_to_owner_only';
  END IF;
  RETURN NULL;
END;
$$;
