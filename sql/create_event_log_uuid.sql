CREATE TABLE
  IF NOT EXISTS event (
    seq_no bigserial PRIMARY KEY,
    entity_id uuid NOT NULL,
    version bigint NOT NULL,
    type_name text NOT NULL,
    event jsonb NOT NULL,
    metadata jsonb NOT NULL,
    UNIQUE (entity_id, type_name, version)
  );
