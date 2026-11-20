BEGIN TRANSACTION;

CREATE TABLE events (
    stream_id           UUID NOT NULL,
    version             INTEGER NOT NULL,
    data_format         events_data_format NOT NULL,
    data_compression    events_data_compression NOT NULL,
    data_encryption     events_data_encryption NOT NULL,
    event_append_ts     TIMESTAMPTZ NOT NULL,
    event_data          BYTEA NOT NULL,
    event_type          TEXT NOT NULL,
    data_encryption_kid TEXT NOT NULL,
    metadata            BYTEA NOT NULL,
    partition_key       TEXT NOT NULL,
    CONSTRAINT pk_events PRIMARY KEY (stream_id, version)
) WITH (fillfactor=90);

ALTER TABLE events
  ALTER COLUMN event_data SET STORAGE EXTERNAL,
  ALTER COLUMN metadata SET STORAGE EXTERNAL;

-- version 0 is the stream's state, stored beside its events by a repository that keeps it
ALTER TABLE events
  ADD CONSTRAINT chk_events_version CHECK (version >= 0);

ALTER TABLE events
  ADD CONSTRAINT chk_events_data_encryption_none
  CHECK ((data_encryption = 'none') = (data_encryption_kid = ''));


GRANT SELECT, INSERT ON events TO :"app";

GRANT UPDATE (data_format, data_compression, data_encryption, event_append_ts,
              event_data, event_type, data_encryption_kid, metadata)
    ON events TO :"app";

-- The conditions are checked before the functions are called, so replacing the state and
-- writing a stream's first event cost no call.
CREATE TRIGGER trg_events_refuse_event_update
    BEFORE UPDATE ON events
    FOR EACH ROW
    WHEN (OLD.version <> 0)
    EXECUTE FUNCTION events_refuse_event_update();

CREATE TRIGGER trg_events_refuse_version_gap
    BEFORE INSERT ON events
    FOR EACH ROW
    WHEN (NEW.version > 1)
    EXECUTE FUNCTION events_refuse_version_gap();


COMMIT TRANSACTION;
