CREATE TABLE
  IF NOT EXISTS events (
    id text,
    seq_no bigint,
    type text,
    event bytea,
    PRIMARY KEY (id, seq_no)
  );