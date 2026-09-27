-- Marquez expects its own database, user and password, all literally
-- "marquez" — marquez.dev.yml inside the image hardcodes them and only
-- makes host and port configurable. This runs once, on first start of
-- an empty data volume.
CREATE USER marquez WITH PASSWORD 'marquez';
CREATE DATABASE marquez OWNER marquez;
GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;
