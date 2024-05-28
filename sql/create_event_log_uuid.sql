CREATE TABLE
  IF NOT EXISTS event (
    id uuid,
    seq_no bigint,
    type text,
    event bytea,
    metadata jsonb,
    PRIMARY KEY (id, seq_no)
  );