BEGIN TRANSACTION;

-- The codec names stored with each event and the functions the events table's triggers call,
-- shared by every journal in the schema, so a journal with its own table names applies this
-- file and creates only its tables and their triggers.

CREATE TYPE events_data_format AS ENUM
    ('json', 'avro', 'cbor', 'ion', 'msgpack', 'proto', 'text', 'xml', 'yaml');

CREATE TYPE events_data_compression AS ENUM
    ('none', 'gzip', 'lz4', 'snappy', 'zstd');

CREATE TYPE events_data_encryption AS ENUM
    ('none', 'aes-gcm', 'xchacha20-poly1305');

-- Keeps every event as it was written, as a grant cannot name rows: only the state, version
-- 0, is ever replaced.
CREATE FUNCTION events_refuse_event_update() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'event % of stream % cannot be updated', OLD.version, OLD.stream_id
        USING ERRCODE = 'insufficient_privilege';
END;
$$;

-- Refuses an event unless the stream stores the version before it, so a stream's versions run
-- from 1 without a gap whoever writes them. It reads the table that fired it, so it serves a
-- table of any name, and sees the rows its own statement inserted before this one.
CREATE FUNCTION events_refuse_version_gap() RETURNS trigger
    LANGUAGE plpgsql AS $$
DECLARE
    previous_stored BOOLEAN;
BEGIN
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.%I WHERE stream_id = $1 AND version = $2)',
                   TG_TABLE_SCHEMA, TG_TABLE_NAME)
        INTO previous_stored
        USING NEW.stream_id, NEW.version - 1;
    IF NOT previous_stored THEN
        RAISE EXCEPTION 'version % of stream % does not follow its version %', NEW.version,
            NEW.stream_id, NEW.version - 1
            USING ERRCODE = 'check_violation', CONSTRAINT = 'events_versions_contiguous',
                  TABLE = TG_TABLE_NAME;
    END IF;
    RETURN NEW;
END;
$$;

COMMIT TRANSACTION;
