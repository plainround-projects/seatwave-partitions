-- The application may read which shards this machine serves (the trigger reads it as the
-- application); only the owner changes it, through a migration or the shard move.
GRANT USAGE ON SCHEMA ${flyway:defaultSchema} TO ${appRole};
GRANT SELECT ON served_shards TO ${appRole};
GRANT EXECUTE ON FUNCTION refuse_unless_owner() TO ${appRole};
