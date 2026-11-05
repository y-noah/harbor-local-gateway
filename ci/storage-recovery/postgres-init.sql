-- The official entrypoint runs this only for a newly initialized, dedicated
-- infrastructure cluster. Existing PGDATA never enters this initialization.
DO $$
DECLARE database_row record;
BEGIN
  FOR database_row IN SELECT datname FROM pg_database LOOP
    EXECUTE format('REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', database_row.datname);
  END LOOP;
END
$$;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
