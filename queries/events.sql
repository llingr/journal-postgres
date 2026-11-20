-- {events_table} and {events_outbox_table} are the store's table names, filled in when it is
-- built

-- name: read-stream
-- Up to row_limit of one stream's events from from_version through through_version, in
-- version order; from_version is 1 or above, so the state row is never read as an event
SELECT stream_id, version, event_type, event_data,
       data_format, data_compression, data_encryption, data_encryption_kid,
       metadata, event_append_ts, partition_key
FROM   {events_table}
WHERE  stream_id = @stream_id
  AND  version BETWEEN @from_version AND @through_version
ORDER BY version
LIMIT  @row_limit;

-- name: read-state
-- The stream's state row, version 0, and its last version in one statement, so both are
-- read at one moment; one row whether or not there is a state, its columns NULL when not
SELECT head.version, state.event_type, state.event_data,
       state.data_format, state.data_compression, state.data_encryption,
       state.data_encryption_kid, state.metadata, state.event_append_ts, state.partition_key
FROM   (SELECT COALESCE(MAX(version), 0) AS version
        FROM   {events_table}
        WHERE  stream_id = @stream_id) AS head
LEFT JOIN {events_table} AS state
       ON state.stream_id = @stream_id
      AND state.version = 0;

-- name: save-state
-- Writes the state row, unless the stored one is folded to a later version. Its metadata is
-- the version folded to and the state version, each four bytes big-endian, so the first four
-- bytes compare as the numbers do. A conflicting writer's row is locked, so the condition is
-- checked against the row as that writer committed it. The partition key is set once.
INSERT INTO {events_table} AS stored (stream_id, version,
                    data_format, data_compression, data_encryption, event_append_ts,
                    event_data, event_type, data_encryption_kid, metadata, partition_key)
VALUES (@stream_id, 0,
        @data_format::events_data_format,
        @data_compression::events_data_compression,
        @data_encryption::events_data_encryption, @event_append_ts,
        @event_data, @event_type, @data_encryption_kid, @metadata, @partition_key)
ON CONFLICT (stream_id, version) DO UPDATE
SET    data_format = EXCLUDED.data_format,
       data_compression = EXCLUDED.data_compression,
       data_encryption = EXCLUDED.data_encryption,
       event_append_ts = EXCLUDED.event_append_ts,
       event_data = EXCLUDED.event_data,
       event_type = EXCLUDED.event_type,
       data_encryption_kid = EXCLUDED.data_encryption_kid,
       metadata = EXCLUDED.metadata
WHERE  substring(stored.metadata FROM 1 FOR 4) <= substring(EXCLUDED.metadata FROM 1 FOR 4);

-- name: insert-events
-- One row per array element, the arrays in step, in their order, as each row's gap check reads
-- the row before it; the codec names arrive as text and are cast to postgres enums
INSERT INTO {events_table} (stream_id, version,
                    data_format, data_compression, data_encryption, event_append_ts,
                    event_data, event_type, data_encryption_kid, metadata, partition_key)
SELECT @stream_id, appended.version,
       appended.data_format::events_data_format,
       appended.data_compression::events_data_compression,
       appended.data_encryption::events_data_encryption, @event_append_ts,
       appended.event_data, appended.event_type, appended.data_encryption_kid, appended.metadata,
       appended.partition_key
FROM   unnest(@versions::INTEGER[], @data_formats::TEXT[], @data_compressions::TEXT[],
              @data_encryptions::TEXT[], @event_data::BYTEA[], @event_types::TEXT[],
              @data_encryption_kids::TEXT[], @metadata::BYTEA[], @partition_keys::TEXT[])
       WITH ORDINALITY
       AS appended(version, data_format, data_compression, data_encryption, event_data,
                   event_type, data_encryption_kid, metadata, partition_key, position)
ORDER BY appended.position;

-- name: insert-outbox
INSERT INTO {events_outbox_table} (stream_id, version, event_append_ts)
SELECT @stream_id, appended.version, @event_append_ts
FROM   unnest(@versions::INTEGER[]) AS appended(version);

-- name: earliest-outbox
-- The stream's lowest version still unpublished below a version, no row when there is none
SELECT version
FROM   {events_outbox_table}
WHERE  stream_id = @stream_id
  AND  version < @version
ORDER BY version
LIMIT  1;

-- name: outbox-versions
SELECT version
FROM   {events_outbox_table}
WHERE  stream_id = @stream_id
ORDER BY version;

-- name: stream-head
-- Read backwards along the primary key, one index entry; 0 for a stream with no events
SELECT COALESCE(MAX(version), 0)
FROM   {events_table}
WHERE  stream_id = @stream_id;

-- name: outbox-streams
-- Rows rather than distinct streams, so the scan stops at the limit
SELECT stream_id
FROM   {events_outbox_table}
WHERE  event_append_ts >= @appended_after
  AND  event_append_ts < @appended_before
LIMIT  @row_limit;

-- name: delete-published
DELETE FROM {events_outbox_table}
WHERE  stream_id = @stream_id
  AND  version <= @version;
