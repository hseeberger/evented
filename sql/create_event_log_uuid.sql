CREATE TABLE
  IF NOT EXISTS event (
    seq_no bigserial PRIMARY KEY,
    entity_id uuid NOT NULL,
    version bigint NOT NULL,
    type text NOT NULL,
    event bytea NOT NULL,
    metadata jsonb NOT NULL,
    UNIQUE (entity_id, version)
  );