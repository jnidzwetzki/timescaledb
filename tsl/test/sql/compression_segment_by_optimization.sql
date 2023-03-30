-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (analyze, verbose, costs off, timing off, summary off)'

CREATE TABLE test1 (
time timestamptz NOT NULL,
x1 integer NOT NULL,
x2 integer NOT NULL,
x3 integer NOT NULL,
x4 integer NOT NULL,
x5 integer NOT NULL);

SELECT FROM create_hypertable('test1', 'time');

ALTER TABLE test1 SET (timescaledb.compress, timescaledb.compress_segmentby='x1, x2, x5', timescaledb.compress_orderby = 'time DESC, x3, x4');

INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 00:00:00-00', 1, 2, 1, 1, 0);
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 01:00:00-00', 1, 3, 2, 2, 0);
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 02:00:00-00', 2, 1, 3, 3, 0);
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 03:00:00-00', 1, 2, 4, 4, 0);

SELECT compress_chunk(c.schema_name|| '.' || c.table_name) FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht where c.hypertable_id = ht.id and ht.table_name = 'test1' and c.compressed_chunk_id IS NULL ORDER BY c.table_name DESC;

-- test1 uses compress_segmentby='x1, x2, x5' and compress_orderby = 'time DESC, x3, x4'

------
-- Tests based on ordering
------

-- Should be optimized 
:PREFIX
SELECT * FROM test1 ORDER BY time DESC;

-- Should be optimized 
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS FIRST;

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS LAST;

-- Should not be optimized (wrong order)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC;

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS FIRST;

------
-- Tests based on attributes
------

-- Should be optimized (some batches qualify by pushed down filter on _ts_meta_max_3)
:PREFIX
SELECT * FROM test1 WHERE x4 > 0 ORDER BY time DESC;

-- Should be optimized (no batches qualify by pushed down filter on _ts_meta_max_3)
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC;

-- Should be optimized 
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x3, x4;

-- Should be optimized (duplicate order by attributes)
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x3, x3;

-- Should be optimized (duplicate order by attributes)
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x3, x4, x3, x4;

-- Should not be optimized 
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x4, x3;

-- Should not be optimized 
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time ASC, x3, x4;


------
-- Tests based on results
------

-- With selection on compressed column (value larger as max value for all batches, so no batch so be opened)
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC;

-- With selection on compressed column (value smaller as max value for some batches, so batches are opened and filter has to be applied)
SELECT * FROM test1 WHERE x4 > 2 ORDER BY time DESC;

-- With selection on segment_by column
SELECT * FROM test1 WHERE time < '1980-01-01 00:00:00-00' ORDER BY time DESC;
SELECT * FROM test1 WHERE time > '1980-01-01 00:00:00-00' ORDER BY time DESC;

-- With selection on segment_by and compressed column
SELECT * FROM test1 WHERE time > '1980-01-01 00:00:00-00' ORDER BY time DESC;
SELECT * FROM test1 WHERE time > '1980-01-01 00:00:00-00' AND x4 > 100 ORDER BY time DESC;

-- Without projection
SELECT * FROM test1 ORDER BY time DESC;

-- With projection on time
SELECT time FROM test1 ORDER BY time DESC;

-- With projection on x3
SELECT x3 FROM test1 ORDER BY time DESC;

-- With projection on x3 and time
SELECT x3,time FROM test1 ORDER BY time DESC;

-- With projection on time and x3
SELECT time,x3 FROM test1 ORDER BY time DESC;

-- With projection and selection on compressed column (value smaller as max value for some batches, so batches are opened and filter has to be applied)
SELECT x4 FROM test1 WHERE x4 > 2 ORDER BY time DESC;

-- Aggregation with count
SELECT count(*) FROM test1;
