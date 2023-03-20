/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H
#define TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H

#include <postgres.h>

#define DECOMPRESS_CHUNK_COUNT_ID -9
#define DECOMPRESS_CHUNK_SEQUENCE_NUM_ID -10

/* The initial capacity of the binary heap */
#define BINARY_HEAP_DEFAULT_CAPACITY 16

/* Initial amount of batch states */
#define INITAL_BATCH_CAPACITY 16

extern Node *decompress_chunk_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H */
