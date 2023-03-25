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

INSERT INTO test1 (time, x1, x2, x3, x4, x5) values(now(), 1, 2, 1, 0, 0);
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values(now(), 1, 3, 2, 0, 0);
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values(now(), 2, 1, 3, 0, 0);
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values(now(), 1, 2, 4, 0, 0);

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

-- Should be optimized 
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
