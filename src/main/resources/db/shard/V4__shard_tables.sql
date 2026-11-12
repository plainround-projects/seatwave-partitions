-- Module 18: what changes when Module 13's schema becomes one of 64 shards. V1 to V3 are Module
-- 13's migrations, copied unchanged; each shard is one schema (shard-17 lives in shard_17) on
-- one of the shard machines. A shard holds the seats, sections, holds and bookings whose
-- (event_id, section_code) hashes to it, and the customers whose "customer-<id>" hashes to it.
-- venues and events are small reference data, copied at event set-up to every shard that holds
-- one of the event's sections, so the foreign key from sections to events stays inside a shard.

-- A hold's customer lives on the customer's shard, usually another one: the foreign key cannot
-- span two databases, so it goes (Section 13's continuity note).
ALTER TABLE holds DROP CONSTRAINT hold_by_a_real_customer;

-- Module 13's four-holds trigger counts the holds on this shard only, so on a shard it would
-- check nothing useful; the rule moves to hold_reservations on the customer's shard (below).
DROP TRIGGER hold_limit_per_customer ON holds;
DROP FUNCTION enforce_hold_limit();

-- Customer ids come from the Snowflake-style generator (IdLayout.seatWave(), a leased worker
-- id), because no single sequence spans 64 shards.
ALTER TABLE customers ALTER COLUMN customer_id DROP IDENTITY;

-- Ids of holds and bookings stay UUIDs (Module 7's contract), now time-ordered: each claim shard
-- makes them with PostgreSQL 18's uuidv7() instead of a random UUID from Java.
ALTER TABLE holds ALTER COLUMN hold_id SET DEFAULT uuidv7();
ALTER TABLE bookings ALTER COLUMN booking_id SET DEFAULT uuidv7();

-- Routes for lookups by id alone (GET /v1/holds/{holdId}, GET /v1/bookings/{bookingId}): the
-- row lives on the shard of the id's own token and names the shard key of the hold or booking.
CREATE TABLE id_routes (
  entity_id UUID PRIMARY KEY,
  event_id BIGINT NOT NULL,
  section_code VARCHAR(4) NOT NULL,
  CONSTRAINT route_section_format CHECK (section_code ~ '^[A-Z]{1,4}$')
);

-- The four-holds rule, moved to the customer's shard (the shard of "customer-<id>"): one row per
-- active hold, written before the seat claim, deleted when the claim fails or the hold ends. The
-- rule's data lives on one shard again, so Module 13's pattern (lock the customer, count,
-- insert) works here unchanged in shape.
CREATE TABLE hold_reservations (
  customer_id BIGINT NOT NULL,
  hold_id UUID NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT hold_reservations_pkey PRIMARY KEY (customer_id, hold_id),
  CONSTRAINT reservation_by_a_real_customer FOREIGN KEY (customer_id)
    REFERENCES customers (customer_id)
);

-- Lock the customer's row, then count: every reservation of one customer waits its turn, and at
-- read committed each statement of this VOLATILE function takes a fresh snapshot, so the count
-- includes every reservation committed before the lock was granted. Repeatable read would count
-- from the transaction's first snapshot, so it is refused, as in Module 13. The clock is the
-- database's: the reservation's expires_at was set from now() on this machine too.
CREATE FUNCTION enforce_reservation_limit() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  reservation_limit CONSTANT INT := TG_ARGV[0]::INT;
  live_reservations INT;
BEGIN
  IF current_setting('transaction_isolation') = 'repeatable read' THEN
    RAISE EXCEPTION 'reservations are written at read committed or serializable, not repeatable read'
      USING ERRCODE = 'feature_not_supported';
  END IF;
  PERFORM 1 FROM customers WHERE customer_id = NEW.customer_id FOR UPDATE;
  SELECT count(*) INTO live_reservations FROM hold_reservations
    WHERE customer_id = NEW.customer_id AND expires_at > now();
  IF live_reservations >= reservation_limit THEN
    RAISE EXCEPTION 'customer % already has % active holds', NEW.customer_id, live_reservations
      USING ERRCODE = 'check_violation', CONSTRAINT = 'reservation_limit_per_customer';
  END IF;
  RETURN NEW;
END;
$$;

-- The argument is the limit: 4 active holds per customer, Module 13's rule.
CREATE TRIGGER reservation_limit_per_customer
  BEFORE INSERT ON hold_reservations
  FOR EACH ROW EXECUTE FUNCTION enforce_reservation_limit('4');

-- "My tickets" for one customer, on the customer's shard: a global secondary index of bookings,
-- written after each confirm (Module 14's "global"; Module 28 makes it reliable).
CREATE TABLE bookings_by_customer (
  customer_id BIGINT NOT NULL,
  confirmed_at TIMESTAMPTZ NOT NULL,
  booking_id UUID NOT NULL,
  event_id BIGINT NOT NULL,
  section_code VARCHAR(4) NOT NULL,
  CONSTRAINT bookings_by_customer_pkey PRIMARY KEY (customer_id, confirmed_at, booking_id),
  CONSTRAINT listed_for_a_real_customer FOREIGN KEY (customer_id)
    REFERENCES customers (customer_id)
);

-- A shard move's "lag 0" check: the move writes one marker row on the old machine after its
-- fence and waits until the row is visible on the new one. Logical replication applies
-- transactions in commit order, so every write committed before the marker has arrived too.
CREATE TABLE move_markers (
  marker_id UUID PRIMARY KEY,
  written_at TIMESTAMPTZ NOT NULL
);

-- Every shard table refuses writes unless this machine owns the shard (routing.served_shards).
-- A statement trigger runs once per statement, not per row, so a bulk load pays it once.
-- Logical replication does not fire ordinary triggers, so a move's copy still arrives at a
-- target that does not accept application writes yet.
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON venues
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON events
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON sections
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON seats
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON customers
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON holds
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON bookings
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON id_routes
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE
  ON hold_reservations
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
CREATE TRIGGER write_to_owner_only BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE
  ON bookings_by_customer
  FOR EACH STATEMENT EXECUTE FUNCTION ${routingSchema}.refuse_unless_owner();
