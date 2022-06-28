CREATE OR REPLACE FUNCTION pg_catalog.is_filtered_citus_object(regclass, regclass)
  RETURNS bool
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$is_filtered_citus_object$$;
COMMENT ON FUNCTION is_filtered_citus_object(regclass, regclass)
    IS 'returns true if the given object for the meta table is a filtered citus object';
