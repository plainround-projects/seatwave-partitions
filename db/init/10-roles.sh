#!/usr/bin/env bash
# Runs once inside each postgres container, as the superuser, when its data volume is new.
# Creates the roles SeatWave uses, so the application never logs in as the superuser:
#   seatwave_owner       owns the schemas and runs migrations (DDL)
#   seatwave_app         reads and writes rows; R__app_grants.sql decides which
#   seatwave_replicator  the login a subscription uses to copy a shard from this machine; it may
#                        start replication and read the shard tables (R__app_grants.sql), no more
# Passwords come from the Compose secrets, as psql variables, never pasted into SQL text.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v owner_password="$(< /run/secrets/owner_password)" \
  -v app_password="$(< /run/secrets/app_password)" \
  -v replicator_password="$(< /run/secrets/replicator_password)" <<'SQL'
CREATE ROLE seatwave_owner LOGIN PASSWORD :'owner_password';
CREATE ROLE seatwave_app LOGIN PASSWORD :'app_password';
CREATE ROLE seatwave_replicator LOGIN REPLICATION PASSWORD :'replicator_password';
-- The owner may create schemas in this database (Flyway creates routing and one per shard).
GRANT CONNECT, CREATE ON DATABASE seatwave TO seatwave_owner;
GRANT CONNECT ON DATABASE seatwave TO seatwave_app, seatwave_replicator;
-- Nobody else may connect: PUBLIC loses the default right.
REVOKE CONNECT ON DATABASE seatwave FROM PUBLIC;
SQL
