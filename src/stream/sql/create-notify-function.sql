CREATE FUNCTION pg_streams_notify() RETURNS trigger AS $trigger$
  DECLARE
  BEGIN
    PERFORM pg_notify(TG_TABLE_NAME || '_channel', NEW.id::text);
    RETURN NEW;
  END;
$trigger$ LANGUAGE plpgsql;
