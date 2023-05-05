/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_DECOMPRESS_VECTOR_CHUNK_EXEC_H
#define TIMESCALEDB_DECOMPRESS_VECTOR_CHUNK_EXEC_H

#include <postgres.h>

typedef struct DecompressChunkVectorState
{
	CustomScanState csstate;
} DecompressChunkVectorState;

extern Node *decompress_chunk_vector_state_create(CustomScan *cscan);

#endif
