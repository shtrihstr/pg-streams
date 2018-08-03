CREATE TRIGGER "insert_trigger" AFTER INSERT ON "{{table}}"
FOR EACH ROW EXECUTE PROCEDURE pg_streams_notify();