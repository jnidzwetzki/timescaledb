/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_operator.h>
#include <miscadmin.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/typcache.h>
#include <utils/fmgroids.h>

#include <planner.h>

#include "compat/compat.h"
#include "debug_assert.h"
#include "ts_catalog/hypertable_compression.h"
#include "import/planner.h"
#include "compression/create.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/qual_pushdown.h"
#include "nodes/decompress_chunk_vector/decompress_chunk_vector.h"
#include "nodes/decompress_chunk_vector/planner.h"
#include "utils.h"

static CustomPathMethods decompress_chunk_vector_path_methods = {
	.CustomName = "DecompressChunk (Vector)",
	.PlanCustomPath = decompress_chunk_vector_plan_create,
};

/* Check if we can vectorize the given path */
static bool
is_vectorizable_agg_path(Path *path)
{
	if (!IsA(path, AggPath))
		return false;

	AggPath *agg_path = castNode(AggPath, path);

	bool is_plain_agg = (agg_path->aggstrategy == AGG_PLAIN);
	if (!is_plain_agg)
		return false;

	bool is_decompress_chunk = ts_is_decompress_chunk_path(agg_path->subpath);
	if (!is_decompress_chunk)
		return false;

	/* We currently handle only one agg function per node */
	if (list_length(agg_path->path.pathtarget->exprs) != 1)
		return false;

	/* Only sum on int 4 is supported at the moment */
	Node *expr_node = linitial(agg_path->path.pathtarget->exprs);
	if (!IsA(expr_node, Aggref))
		return false;

	Aggref *aggref = castNode(Aggref, expr_node);

#if PG14_LT
	if (aggref->aggfnoid != F_INT4_SUM)
#else
	if (aggref->aggfnoid != F_SUM_INT4)
#endif
		return false;

	return true;
}

/* Generate cheaper path with our vector node */
static void
change_to_vector_path(PlannerInfo *root, RelOptInfo *output_rel, AggPath *aggregation_path,
					  List *subpaths)
{
	Assert(root != NULL);
	Assert(subpaths != NULL);

	ListCell *lc;

	/* Check if subpaths can be vectorized */
	foreach (lc, subpaths)
	{
		Path *sub_path = lfirst(lc);

		if (is_vectorizable_agg_path(sub_path))
		{
			Assert(IsA(sub_path, AggPath));
			AggPath *agg_path = castNode(AggPath, sub_path);

			Assert(ts_is_decompress_chunk_path(agg_path->subpath));
			DecompressChunkPath *decompress_path =
				(DecompressChunkPath *) castNode(CustomPath, agg_path->subpath);

			Assert(decompress_path != NULL);

			DecompressChunkVectorPath *vector_path =
				(DecompressChunkVectorPath *) newNode(sizeof(DecompressChunkVectorPath),
													  T_CustomPath);

			// TODO: Get planner data from subpath
			vector_path->custom_path = decompress_path->custom_path;
			vector_path->info = decompress_path->info;

			/* Set the target to our custom vector node */
			vector_path->custom_path.methods = &decompress_chunk_vector_path_methods;

			/* Our node should emit partials */
			vector_path->custom_path.path.pathtarget = aggregation_path->path.pathtarget;

			Assert(vector_path != NULL);
			lfirst(lc) = vector_path;
		}
	}
}

static void
handle_agg_sub_path(PlannerInfo *root, RelOptInfo *output_rel, AggPath *aggregation_path,
					Path *agg_sub_path)
{
	Assert(agg_sub_path != NULL);

	if (IsA(agg_sub_path, AppendPath))
	{
		AppendPath *append_path = castNode(AppendPath, agg_sub_path);
		List *subpaths = append_path->subpaths;

		/* No subpaths available */
		if (list_length(subpaths) < 1)
			return;

		change_to_vector_path(root, output_rel, aggregation_path, subpaths);
	}
	else if (IsA(agg_sub_path, MergeAppendPath))
	{
		MergeAppendPath *merge_append_path = castNode(MergeAppendPath, agg_sub_path);
		List *subpaths = merge_append_path->subpaths;

		/* No subpaths available */
		if (list_length(subpaths) < 1)
			return;

		change_to_vector_path(root, output_rel, aggregation_path, subpaths);
	}
	else if (IsA(agg_sub_path, GatherPath))
	{
		/* Handle parallel plans with a gather node on top */
		GatherPath *gather_path = castNode(GatherPath, agg_sub_path);

		// TODO: Maybe extract AGGPATH subpath also here

		if (gather_path->subpath != NULL)
			handle_agg_sub_path(root, output_rel, aggregation_path, gather_path->subpath);
	}
}

/*
 * This function searches for a partial aggregation node on top of a DecompressChunk node
 * and replace it by our DecompressChunkVector node.
 *
 * For example
 * ->  Append  (cost=253.00..2036.08 rows=5 width=8) (actual time=13.610..180.192 rows=5 loops=1)
 *    ->  Partial Aggregate  (cost=304.18..304.19 rows=1 width=8)
 *           ->  Custom Scan (DecompressChunk) on _hyper_34_35_chunk  (cost=0.08..9.18 rows=118000
 * width=4)
 *                ->  Parallel Seq Scan on compress_hyper_35_42_chunk  (cost=0.00..9.18 rows=118
 * width=8)
 *
 * Will be replaced by
 * ->  Append  (cost=253.00..2036.08 rows=5 width=8) (actual time=13.610..180.192 rows=5 loops=1)
 *    ->  Custom Scan (VectorDecompressChunk) on _hyper_34_35_chunk  (cost=0.08..9.18 rows=118000
 * width=4)
 *           ->  Parallel Seq Scan on compress_hyper_35_42_chunk  (cost=0.00..9.18 rows=118 width=8)
 */
void
ts_decompress_vector_modify_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	Assert(root != NULL);
	Assert(input_rel != NULL);
	Assert(output_rel != NULL);

	if (output_rel->reloptkind != RELOPT_UPPER_REL)
		return;

	ListCell *lc;
	foreach (lc, output_rel->pathlist)
	{
		/* We are only interested in AggPaths */
		if (!IsA(lfirst(lc), AggPath))
			continue;

		AggPath *aggregation_path = lfirst_node(AggPath, lc);

		/* We are only interested in splitted paths */
		if (aggregation_path->aggsplit != AGGSPLIT_FINAL_DESERIAL)
			continue;

		/* Handle the subpath of the aggregation */
		Path *agg_sub_path = aggregation_path->subpath;

		handle_agg_sub_path(root, output_rel, aggregation_path, agg_sub_path);
	}
}
