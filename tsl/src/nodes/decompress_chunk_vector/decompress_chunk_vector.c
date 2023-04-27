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

#include <planner.h>

#include "compat/compat.h"
#include "debug_assert.h"
#include "ts_catalog/hypertable_compression.h"
#include "import/planner.h"
#include "compression/create.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/planner.h"
#include "nodes/decompress_chunk/qual_pushdown.h"
#include "nodes/decompress_chunk_vector/decompress_chunk_vector.h"
#include "utils.h"

void
ts_decompress_vector_modify_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	Assert(root != NULL);
	Assert(input_rel != NULL);
	Assert(output_rel != NULL);
	Assert(output_rel->reloptkind == RELOPT_UPPER_REL);

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

		Path *aggregation_sub_path = aggregation_path->subpath;
		Assert(aggregation_sub_path);
	}
}
