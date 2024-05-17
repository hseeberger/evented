CREATE TABLE
  IF NOT EXISTS event (
    id uuid,
    seq_no bigint,
    type text,
    event bytea,
    PRIMARY KEY (id, seq_no)
  );