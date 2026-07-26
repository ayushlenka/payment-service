-- Runs once, on first initialization of an empty Postgres data directory.
-- The "payments" database is already created by POSTGRES_DB; here we add the
-- "ledger" database so each service owns an isolated database.
--
-- Both are owned by the same "payments" role for local simplicity. In a real
-- deployment each service would have its own credentials with access only to
-- its own database.
CREATE DATABASE ledger OWNER payments;
