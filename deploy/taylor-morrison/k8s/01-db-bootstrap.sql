-- 01-db-bootstrap.sql
CREATE ROLE tmvault WITH LOGIN PASSWORD :'pw';
CREATE DATABASE tmvault_prod OWNER tmvault;
\connect tmvault_prod
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid() used by schema
GRANT ALL ON SCHEMA public TO tmvault;
-- App migrations target the dedicated tm_vault schema (DB_SCHEMA), not public.
CREATE SCHEMA IF NOT EXISTS tm_vault AUTHORIZATION tmvault;
