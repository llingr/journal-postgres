BEGIN TRANSACTION;

CREATE TABLE events_outbox (
    stream_id        UUID NOT NULL,
    version          INTEGER NOT NULL,
    event_append_ts  TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_events_outbox PRIMARY KEY (stream_id, version)
);

ALTER TABLE events_outbox
  ADD CONSTRAINT chk_events_outbox_version CHECK (version > 0);

GRANT SELECT, INSERT, DELETE
    ON events_outbox TO :"app";

COMMIT TRANSACTION;
