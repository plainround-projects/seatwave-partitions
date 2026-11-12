-- The application's role may read and write SeatWave's rows and nothing else: no DDL, no access to
-- Flyway's history table, and DELETE only on hold_reservations, whose row is removed when a claim
-- fails or a hold ends (a hold or a booking is never deleted, only ended, as in Module 13).
-- The replicator may read the shard's tables, which is what a subscription's initial copy needs.
GRANT USAGE ON SCHEMA ${flyway:defaultSchema} TO ${appRole}, ${replicatorRole};
GRANT SELECT ON venues, events, sections, seats TO ${appRole};
GRANT UPDATE (price, version) ON sections TO ${appRole};
-- UPDATE on customers is what lets the reservation trigger lock the customer's row.
GRANT SELECT, INSERT, UPDATE ON customers, holds, bookings TO ${appRole};
GRANT SELECT, INSERT ON id_routes, bookings_by_customer TO ${appRole};
GRANT SELECT, INSERT, DELETE ON hold_reservations TO ${appRole};
GRANT SELECT ON venues, events, sections, seats, customers, holds, bookings, id_routes,
  hold_reservations, bookings_by_customer, move_markers TO ${replicatorRole};
