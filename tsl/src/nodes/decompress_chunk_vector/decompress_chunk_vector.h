/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_DECOMPRESS_CHUNK_VECTOR_H
#define TIMESCALEDB_DECOMPRESS_CHUNK_VECTOR_H

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>

#include "chunk.h"
#include "hypertable.h"
#include "nodes/decompress_chunk/decompress_chunk.h"

typedef struct DecompressChunkVectorPath
{
	CustomPath cpath;
	CompressionInfo *info;
} DecompressChunkVectorPath;

extern void ts_decompress_vector_modify_paths(PlannerInfo *root, RelOptInfo *input_rel,
											  RelOptInfo *output_rel);

#endif
