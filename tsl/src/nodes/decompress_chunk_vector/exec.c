/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <miscadmin.h>
#include <access/sysattr.h>
#include <executor/executor.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/memutils.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "compression/array.h"
#include "compression/compression.h"
#include "nodes/decompress_chunk_vector/exec.h"
#include "ts_catalog/hypertable_compression.h"

static TupleTableSlot *decompress_chunk_vector_exec(CustomScanState *node);
static void decompress_chunk_vector_begin(CustomScanState *node, EState *estate, int eflags);
static void decompress_chunk_vector_end(CustomScanState *node);
static void decompress_chunk_vector_rescan(CustomScanState *node);
static void decompress_chunk_vector_explain(CustomScanState *node, List *ancestors,
											ExplainState *es);

static CustomExecMethods decompress_chunk_vector_state_methods = {
	.BeginCustomScan = decompress_chunk_vector_begin,
	.ExecCustomScan = decompress_chunk_vector_exec,
	.EndCustomScan = decompress_chunk_vector_end,
	.ReScanCustomScan = decompress_chunk_vector_rescan,
	.ExplainCustomScan = decompress_chunk_vector_explain,
};

static TupleTableSlot *
decompress_chunk_vector_exec(CustomScanState *node)
{
	return NULL;
}

static void
decompress_chunk_vector_begin(CustomScanState *node, EState *estate, int eflags)
{
}

static void
decompress_chunk_vector_end(CustomScanState *node)
{
}

static void
decompress_chunk_vector_rescan(CustomScanState *node)
{
}

static void
decompress_chunk_vector_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
}

Node *
decompress_chunk_vector_state_create(CustomScan *cscan)
{
	DecompressChunkVectorState *chunk_state;
	chunk_state = (DecompressChunkVectorState *) newNode(sizeof(DecompressChunkVectorState),
														 T_CustomScanState);
	chunk_state->csstate.methods = &decompress_chunk_vector_state_methods;
	return (Node *) chunk_state;
}
