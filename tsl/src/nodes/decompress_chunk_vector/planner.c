/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/sysattr.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/plancat.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "nodes/decompress_chunk_vector/decompress_chunk_vector.h"
#include "nodes/decompress_chunk_vector/planner.h"
#include "nodes/decompress_chunk_vector/exec.h"
#include "utils.h"

static CustomScanMethods decompress_chunk_plan_methods = {
	.CustomName = "DecompressChunk (Vector)",
	.CreateCustomScanState = decompress_chunk_vector_state_create,
};

void
_decompress_chunk_vector_init(void)
{
	TryRegisterCustomScanMethods(&decompress_chunk_plan_methods);
}

Plan *
decompress_chunk_vector_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
									List *decompressed_tlist, List *clauses, List *custom_plans)
{
	DecompressChunkVectorPath *dcpath = (DecompressChunkVectorPath *) path;
	CustomScan *decompress_plan = makeNode(CustomScan);

	// TODO: Copy information from path to plan

	decompress_plan->flags = path->flags;
	decompress_plan->methods = &decompress_chunk_plan_methods;

	/* Relid */
	decompress_plan->scan.scanrelid = dcpath->info->chunk_rel->relid;

	/* Targetlist */
	decompress_plan->scan.plan.targetlist = decompressed_tlist;
	decompress_plan->custom_scan_tlist = decompressed_tlist;

	// FIXME "Aggref found in non-Agg plan node"
	// decompress_plan->custom_scan_tlist = NIL;

	decompress_plan->custom_plans = custom_plans;

	return &decompress_plan->scan.plan;
}
