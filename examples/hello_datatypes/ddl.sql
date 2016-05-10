-- table with one of each datatype
CREATE TABLE hello_types (
  id_int INTEGER NOT NULL,
  --id_tinyint TINYINT,
  --id_smallint SMALLINT,
  --id_bigint BIGINT,
  --col_float FLOAT,
  --col_dec DECIMAL,
  --col_varb VARBINARY(50),
  --msg VARCHAR(20),
  ts TIMESTAMP,
  PRIMARY KEY (id_int)
);
PARTITION TABLE hello_types ON COLUMN id_int;
