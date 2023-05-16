/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_DECOMPRESS_CHUNK_VECTOR_PLANNER_H
#define TIMESCALEDB_DECOMPRESS_CHUNK_VECTOR_PLANNER_H

#include <postgres.h>

extern Plan *decompress_chunk_vector_plan_create(PlannerInfo *root, RelOptInfo *rel,
												 CustomPath *path, List *decompressed_tlist,
												 List *clauses, List *custom_plans);

extern void _decompress_chunk_vector_init(void);

#endif
