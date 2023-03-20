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
#include <lib/binaryheap.h>

#include "compat/compat.h"
#include "compression/array.h"
#include "compression/compression.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/planner.h"
#include "ts_catalog/hypertable_compression.h"

typedef enum DecompressChunkColumnType
{
	SEGMENTBY_COLUMN,
	COMPRESSED_COLUMN,
	COUNT_COLUMN,
	SEQUENCE_NUM_COLUMN,
} DecompressChunkColumnType;

typedef struct DecompressChunkColumnState
{
	DecompressChunkColumnType type;
	Oid typid;

	/*
	 * Attno of the decompressed column in the output of DecompressChunk node.
	 * Negative values are special columns that do not have a representation in
	 * the uncompressed chunk, but are still used for decompression. They should
	 * have the respective `type` field.
	 */
	AttrNumber output_attno;

	/*
	 * Attno of the compressed column in the input compressed chunk scan.
	 */
	AttrNumber compressed_scan_attno;

	union
	{
		struct
		{
			Datum value;
			bool isnull;
			int count;
		} segmentby;
		struct
		{
			DecompressionIterator *iterator;
		} compressed;
	};
} DecompressChunkColumnState;

typedef struct DecompressBatchState
{
	TupleTableSlot *uncompressed_tuple_slot;
	TupleTableSlot *segment_slot;
	DecompressChunkColumnState *columns;
	int counter;
	MemoryContext per_batch_context;
} DecompressBatchState;

typedef struct DecompressChunkState
{
	CustomScanState csstate;
	List *decompression_map;
	int num_columns;

	bool initialized;
	bool reverse;
	int hypertable_id;
	Oid chunk_relid;
	List *hypertable_compression_info;

	/* Per batch states */
	int no_batch_states;				/* number of batch states */
	DecompressBatchState *batch_states; /* the batch states */
	Bitmapset *unused_batch_states;		/* the unused batch states */

	bool segment_merge_append;	   /* Merge append optimization */
	struct binaryheap *merge_heap; /* binary heap of slot indices */

	/* Sort keys for heap merge function */
	int no_sortkeys;
	SortSupportData *sortkeys;
} DecompressChunkState;

/*
 * From nodeMergeAppend.c
 *
 * We have one slot for each item in the heap array.  We use SlotNumber
 * to store slot indexes.  This doesn't actually provide any formal
 * type-safety, but it makes the code more self-documenting.
 */
typedef int32 SlotNumber;

static TupleTableSlot *decompress_chunk_exec(CustomScanState *node);
static void decompress_chunk_begin(CustomScanState *node, EState *estate, int eflags);
static void decompress_chunk_end(CustomScanState *node);
static void decompress_chunk_rescan(CustomScanState *node);
static void decompress_chunk_explain(CustomScanState *node, List *ancestors, ExplainState *es);
static void decompress_chunk_create_tuple(DecompressChunkState *chunk_state,
										  DecompressBatchState *batch_state, TupleTableSlot *slot);
static void decompress_next_tuple_from_batch(DecompressChunkState *chunk_state,
											 DecompressBatchState *batch_state,
											 TupleTableSlot *slot);
static void initialize_column_state(DecompressChunkState *chunk_state,
									DecompressBatchState *batch_state);

static CustomExecMethods decompress_chunk_state_methods = {
	.BeginCustomScan = decompress_chunk_begin,
	.ExecCustomScan = decompress_chunk_exec,
	.EndCustomScan = decompress_chunk_end,
	.ReScanCustomScan = decompress_chunk_rescan,
	.ExplainCustomScan = decompress_chunk_explain,
};

Node *
decompress_chunk_state_create(CustomScan *cscan)
{
	DecompressChunkState *chunk_state;
	List *settings;

	chunk_state = (DecompressChunkState *) newNode(sizeof(DecompressChunkState), T_CustomScanState);

	chunk_state->csstate.methods = &decompress_chunk_state_methods;

	settings = linitial(cscan->custom_private);
	Assert(list_length(settings) == 5);

	chunk_state->hypertable_id = linitial_int(settings);
	chunk_state->chunk_relid = lsecond_int(settings);
	chunk_state->reverse = lthird_int(settings);
	chunk_state->segment_merge_append = lfourth_int(settings);
	chunk_state->no_sortkeys = llast_int(settings);

	chunk_state->decompression_map = lsecond(cscan->custom_private);
	chunk_state->sortkeys = lthird(cscan->custom_private);

	/* Sort keys should only be present when segment_merge_append is used*/
	Assert(chunk_state->segment_merge_append == true || chunk_state->no_sortkeys == 0);
	Assert(chunk_state->no_sortkeys == 0 || chunk_state->sortkeys != NULL);

	return (Node *) chunk_state;
}

/*
 * Create states to hold up to n batches
 */
static void
batch_states_create(DecompressChunkState *chunk_state, int nbatches)
{
	Assert(nbatches >= 0);

	chunk_state->no_batch_states = nbatches;
	chunk_state->batch_states = palloc0(sizeof(DecompressBatchState) * nbatches);

	for (int segment = 0; segment < nbatches; segment++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[segment];
		initialize_column_state(chunk_state, batch_state);
	}

	chunk_state->unused_batch_states =
		bms_add_range(chunk_state->unused_batch_states, 0, nbatches - 1);
}

/*
 * Enhance the capacity of parallel open batches
 */
static void
batch_states_enlarge(DecompressChunkState *chunk_state, int nbatches)
{
	Assert(nbatches > chunk_state->no_batch_states);
	chunk_state->batch_states =
		(DecompressBatchState *) repalloc(chunk_state->batch_states,
										  sizeof(DecompressBatchState) * nbatches);

	/* Init new batch states */
	for (int segment = chunk_state->no_batch_states; segment < nbatches; segment++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[segment];
		initialize_column_state(chunk_state, batch_state);
	}

	chunk_state->unused_batch_states =
		bms_add_range(chunk_state->unused_batch_states, chunk_state->no_batch_states, nbatches - 1);
	chunk_state->no_batch_states = nbatches;
}

/*
 * Mark a DecompressBatchState as unused
 */
/*static void
set_batch_state_to_unused(DecompressChunkState *chunk_state, int batch_id)
{
	Assert(batch_id < chunk_state->no_batch_states);
	Assert(! bms_is_member(batch_id, chunk_state -> unused_batch_states));
	chunk_state -> unused_batch_states = bms_add_member(chunk_state -> unused_batch_states,
batch_id);
}*/

/*
 * Get the next free and unused batch state and mark as used
 */
static SlotNumber
get_next_unused_batch_state_id(DecompressChunkState *chunk_state)
{
	if (bms_is_empty(chunk_state->unused_batch_states))
		batch_states_enlarge(chunk_state, chunk_state->no_batch_states + INITAL_BATCH_CAPACITY);

	Assert(!bms_is_empty(chunk_state->unused_batch_states));

	SlotNumber next_free_batch = bms_next_member(chunk_state->unused_batch_states, -1);
	Assert(next_free_batch >= 0);

	bms_del_member(chunk_state->unused_batch_states, next_free_batch);

	return next_free_batch;
}

/*
 * initialize column chunk_state
 *
 * the column chunk_state indexes are based on the index
 * of the columns of the uncompressed chunk because
 * that is the tuple layout we are creating
 */
static void
initialize_column_state(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	ScanState *ss = (ScanState *) chunk_state;
	TupleDesc desc = ss->ss_ScanTupleSlot->tts_tupleDescriptor;
	ListCell *lc;

	if (list_length(chunk_state->decompression_map) == 0)
	{
		elog(ERROR, "no columns specified to decompress");
	}

	batch_state->per_batch_context = AllocSetContextCreate(CurrentMemoryContext,
														   "DecompressChunk per_batch",
														   ALLOCSET_DEFAULT_SIZES);

	batch_state->columns =
		palloc0(list_length(chunk_state->decompression_map) * sizeof(DecompressChunkColumnState));

	AttrNumber next_compressed_scan_attno = 0;
	chunk_state->num_columns = 0;
	foreach (lc, chunk_state->decompression_map)
	{
		next_compressed_scan_attno++;

		AttrNumber output_attno = lfirst_int(lc);
		if (output_attno == 0)
		{
			/* We are asked not to decompress this column, skip it. */
			continue;
		}

		DecompressChunkColumnState *column = &batch_state->columns[chunk_state->num_columns];
		chunk_state->num_columns++;

		column->output_attno = output_attno;
		column->compressed_scan_attno = next_compressed_scan_attno;

		if (output_attno > 0)
		{
			/* normal column that is also present in uncompressed chunk */
			Form_pg_attribute attribute =
				TupleDescAttr(desc, AttrNumberGetAttrOffset(output_attno));
			FormData_hypertable_compression *ht_info =
				get_column_compressioninfo(chunk_state->hypertable_compression_info,
										   NameStr(attribute->attname));

			column->typid = attribute->atttypid;

			if (ht_info->segmentby_column_index > 0)
				column->type = SEGMENTBY_COLUMN;
			else
				column->type = COMPRESSED_COLUMN;
		}
		else
		{
			/* metadata columns */
			switch (column->output_attno)
			{
				case DECOMPRESS_CHUNK_COUNT_ID:
					column->type = COUNT_COLUMN;
					break;
				case DECOMPRESS_CHUNK_SEQUENCE_NUM_ID:
					column->type = SEQUENCE_NUM_COLUMN;
					break;
				default:
					elog(ERROR, "Invalid column attno \"%d\"", column->output_attno);
					break;
			}
		}
	}
}

typedef struct ConstifyTableOidContext
{
	Index chunk_index;
	Oid chunk_relid;
	bool made_changes;
} ConstifyTableOidContext;

static Node *
constify_tableoid_walker(Node *node, ConstifyTableOidContext *ctx)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = castNode(Var, node);

		if ((Index) var->varno != ctx->chunk_index)
			return node;

		if (var->varattno == TableOidAttributeNumber)
		{
			ctx->made_changes = true;
			return (
				Node *) makeConst(OIDOID, -1, InvalidOid, 4, (Datum) ctx->chunk_relid, false, true);
		}

		/*
		 * we doublecheck system columns here because projection will
		 * segfault if any system columns get through
		 */
		if (var->varattno < SelfItemPointerAttributeNumber)
			elog(ERROR, "transparent decompression only supports tableoid system column");

		return node;
	}

	return expression_tree_mutator(node, constify_tableoid_walker, (void *) ctx);
}

static List *
constify_tableoid(List *node, Index chunk_index, Oid chunk_relid)
{
	ConstifyTableOidContext ctx = {
		.chunk_index = chunk_index,
		.chunk_relid = chunk_relid,
		.made_changes = false,
	};

	List *result = (List *) constify_tableoid_walker((Node *) node, &ctx);
	if (ctx.made_changes)
	{
		return result;
	}

	return node;
}

/*
 * Complete initialization of the supplied CustomScanState.
 *
 * Standard fields have been initialized by ExecInitCustomScan,
 * but any private fields should be initialized here.
 */
static void
decompress_chunk_begin(CustomScanState *node, EState *estate, int eflags)
{
	DecompressChunkState *state = (DecompressChunkState *) node;
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Plan *compressed_scan = linitial(cscan->custom_plans);
	Assert(list_length(cscan->custom_plans) == 1);

	PlanState *ps = &node->ss.ps;
	if (ps->ps_ProjInfo)
	{
		/*
		 * if we are projecting we need to constify tableoid references here
		 * because decompressed tuple are virtual tuples and don't have
		 * system columns.
		 *
		 * We do the constify in executor because even after plan creation
		 * our targetlist might still get modified by parent nodes pushing
		 * down targetlist.
		 */
		List *tlist = ps->plan->targetlist;
		List *modified_tlist = constify_tableoid(tlist, cscan->scan.scanrelid, state->chunk_relid);

		if (modified_tlist != tlist)
		{
			ps->ps_ProjInfo =
				ExecBuildProjectionInfo(modified_tlist,
										ps->ps_ExprContext,
										ps->ps_ResultTupleSlot,
										ps,
										node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
		}
	}

	state->hypertable_compression_info = ts_hypertable_compression_get(state->hypertable_id);

	node->custom_ps = lappend(node->custom_ps, ExecInitNode(compressed_scan, estate, eflags));
}

static void
initialize_batch(DecompressChunkState *chunk_state, DecompressBatchState *batch_state,
				 TupleTableSlot *slot)
{
	Datum value;
	bool isnull;
	int i;

	MemoryContext old_context = MemoryContextSwitchTo(batch_state->per_batch_context);
	MemoryContextReset(batch_state->per_batch_context);

	for (i = 0; i < chunk_state->num_columns; i++)
	{
		DecompressChunkColumnState *column = &batch_state->columns[i];

		switch (column->type)
		{
			case COMPRESSED_COLUMN:
			{
				value = slot_getattr(slot, column->compressed_scan_attno, &isnull);
				if (!isnull)
				{
					CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(value);

					column->compressed.iterator =
						tsl_get_decompression_iterator_init(header->compression_algorithm,
															chunk_state->reverse)(PointerGetDatum(
																					  header),
																				  column->typid);
				}
				else
					column->compressed.iterator = NULL;

				break;
			}
			case SEGMENTBY_COLUMN:
				value = slot_getattr(slot, column->compressed_scan_attno, &isnull);
				if (!isnull)
					column->segmentby.value = value;
				else
					column->segmentby.value = (Datum) 0;

				column->segmentby.isnull = isnull;
				break;
			case COUNT_COLUMN:
				value = slot_getattr(slot, column->compressed_scan_attno, &isnull);
				batch_state->counter = DatumGetInt32(value);
				/* count column should never be NULL */
				Assert(!isnull);
				break;
			case SEQUENCE_NUM_COLUMN:
				/*
				 * nothing to do here for sequence number
				 * we only needed this for sorting in node below
				 */
				break;
		}
	}
	chunk_state->initialized = true;
	MemoryContextSwitchTo(old_context);
}

/*
 * Compare the tuples of two given slots.
 */
static int32
heap_compare_slots(Datum a, Datum b, void *arg)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) arg;
	SlotNumber batchA = DatumGetInt32(a);
	Assert(batchA <= chunk_state->no_batch_states);

	SlotNumber batchB = DatumGetInt32(b);
	Assert(batchB <= chunk_state->no_batch_states);

	TupleTableSlot *tupleA = chunk_state->batch_states[batchA].uncompressed_tuple_slot;
	Assert(!TupIsNull(tupleA));

	TupleTableSlot *tupleB = chunk_state->batch_states[batchB].uncompressed_tuple_slot;
	Assert(!TupIsNull(tupleB));

	for (int nkey = 0; nkey < chunk_state->no_sortkeys; nkey++)
	{
		SortSupportData *sortKey = &chunk_state->sortkeys[nkey];
		Assert(sortKey != NULL);
		AttrNumber attno = sortKey->ssup_attno;

		bool isNullA, isNullB;

		Datum datumA = slot_getattr(tupleA, attno, &isNullA);
		Datum datumB = slot_getattr(tupleB, attno, &isNullB);

		int compare = ApplySortComparator(datumA, isNullA, datumB, isNullB, sortKey);

		if (compare != 0)
		{
			INVERT_COMPARE_RESULT(compare);
			return compare;
		}
	}

	return 0;
}

// TODO
// * [ ] Optimize multiple batches per segment
// * [ ] what is about the HT partitioning?
// * [ ] Write/Enhance test cases

/* Add a new datum to the heap. In contrast to the
 * binaryheap_add_unordered() function, the capacity
 * of the heap is automatically increased if needed.
 */
static pg_nodiscard binaryheap *
add_to_binary_heap_autoresize(binaryheap *heap, Datum d)
{
	/* Resize heap if needed */
	if (heap->bh_size >= heap->bh_space)
	{
		heap->bh_space = heap->bh_space * 2;
		Size new_size = offsetof(binaryheap, bh_nodes) + sizeof(Datum) * heap->bh_space;
		heap = (binaryheap *) repalloc(heap, new_size);
	}

	/* Insert new element */
	binaryheap_add_unordered(heap, d);

	return heap;
}

static TupleTableSlot *
decompress_chunk_exec(CustomScanState *node)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;

	if (node->custom_ps == NIL)
		return NULL;

	/* If the segment_merge_apend flag is set, the compression order_by and the
	 * query order_by do match. Therefore, we use a binary heap to decompress
	 * and merge the tuples.
	 */
	if (chunk_state->segment_merge_append)
	{
		/* Create the heap on the first call. */
		if (chunk_state->merge_heap == NULL)
		{
			/* Prepare the heap and the batch states */
			chunk_state->merge_heap =
				binaryheap_allocate(BINARY_HEAP_DEFAULT_CAPACITY, heap_compare_slots, chunk_state);
			batch_states_create(chunk_state, INITAL_BATCH_CAPACITY);

			while (true)
			{
				TupleTableSlot *subslot = ExecProcNode(linitial(chunk_state->csstate.custom_ps));

				if (TupIsNull(subslot))
					break;

				SlotNumber batch_state_id = get_next_unused_batch_state_id(chunk_state);
				DecompressBatchState *batch_state = &chunk_state->batch_states[batch_state_id];

				TupleDesc tdesc_sub = CreateTupleDescCopy(subslot->tts_tupleDescriptor);
				batch_state->segment_slot = MakeSingleTupleTableSlot(tdesc_sub, subslot->tts_ops);
				ExecCopySlot(batch_state->segment_slot, subslot);

				Assert(!TupIsNull(batch_state->segment_slot));
				initialize_batch(chunk_state, batch_state, batch_state->segment_slot);

				TupleTableSlot *slot = chunk_state->csstate.ss.ss_ScanTupleSlot;
				TupleDesc tdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
				batch_state->uncompressed_tuple_slot =
					MakeSingleTupleTableSlot(tdesc, slot->tts_ops);

				decompress_next_tuple_from_batch(chunk_state,
												 batch_state,
												 batch_state->uncompressed_tuple_slot);

				Assert(!TupIsNull(batch_state->uncompressed_tuple_slot));

				chunk_state->merge_heap =
					add_to_binary_heap_autoresize(chunk_state->merge_heap,
												  Int32GetDatum(batch_state_id));
			}

			elog(WARNING, "Heap has capacity of %d", chunk_state->merge_heap->bh_space);
			elog(WARNING, "Created batch states %d", chunk_state->no_batch_states);

			binaryheap_build(chunk_state->merge_heap);
		}
		else
		{
			/* Remove the tuple we have returned last time and decompress
			 * the next tuple from the segment. This operation is delayed
			 * up to this point where the next tuple actually needs to be
			 * decompressed.
			 */
			SlotNumber i = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));

			/* Decompress the next tuple from segment */
			DecompressBatchState *batch_state = &chunk_state->batch_states[i];

			decompress_next_tuple_from_batch(chunk_state,
											 batch_state,
											 batch_state->uncompressed_tuple_slot);

			/* Put the next tuple into the heap */
			if (TupIsNull(batch_state->uncompressed_tuple_slot))
				(void) binaryheap_remove_first(chunk_state->merge_heap);
			else
				binaryheap_replace_first(chunk_state->merge_heap, Int32GetDatum(i));
		}

		/* All tuples are decompressed */
		if (binaryheap_empty(chunk_state->merge_heap))
			return NULL;

		/* Return the next tuple from our heap. */
		SlotNumber i = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));
		Assert(i >= 0);

		/* Fetch tuple from slot */
		TupleTableSlot *result = chunk_state->batch_states[i].uncompressed_tuple_slot;
		Assert(result != NULL);

		return result;
	}
	else
	{
		if (chunk_state->batch_states == NULL)
			batch_states_create(chunk_state, 1);

		while (true)
		{
			DecompressBatchState *batch_state = &chunk_state->batch_states[0];
			TupleTableSlot *slot = chunk_state->csstate.ss.ss_ScanTupleSlot;
			decompress_chunk_create_tuple(chunk_state, batch_state, slot);

			if (TupIsNull(slot))
				return NULL;

			econtext->ecxt_scantuple = slot;

			/* Reset expression memory context to clean out any cruft from
			 * previous tuple. */
			ResetExprContext(econtext);

			if (node->ss.ps.qual && !ExecQual(node->ss.ps.qual, econtext))
			{
				InstrCountFiltered1(node, 1);
				ExecClearTuple(slot);
				continue;
			}

			if (!node->ss.ps.ps_ProjInfo)
				return slot;

			return ExecProject(node->ss.ps.ps_ProjInfo);
		}
	}
}

static void
decompress_chunk_rescan(CustomScanState *node)
{
	((DecompressChunkState *) node)->initialized = false;
	ExecReScan(linitial(node->custom_ps));
}

/* End the decompress operation and free the requested resources */
static void
decompress_chunk_end(CustomScanState *node)
{
	int i;
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	for (i = 0; i < chunk_state->no_batch_states; i++)
	{
		DecompressBatchState *batch_state = &chunk_state->batch_states[i];

		/* State is unused */
		if (batch_state == NULL)
			continue;

		if (batch_state->segment_slot)
			ExecDropSingleTupleTableSlot(batch_state->segment_slot);

		if (batch_state->uncompressed_tuple_slot)
			ExecDropSingleTupleTableSlot(batch_state->uncompressed_tuple_slot);

		batch_state = NULL;
	}

	ExecEndNode(linitial(node->custom_ps));
}

/*
 * Output additional information for EXPLAIN of a custom-scan plan node.
 */
static void
decompress_chunk_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) node;

	if (es->verbose || es->format != EXPLAIN_FORMAT_TEXT)
		ExplainPropertyBool("Per segment merge append", chunk_state->segment_merge_append, es);
}

static void
decompress_next_tuple_from_batch(DecompressChunkState *chunk_state,
								 DecompressBatchState *batch_state, TupleTableSlot *slot)
{
	int i;
	bool batch_done = false;

	/* Clear old slot state */
	ExecClearTuple(slot);

	for (i = 0; i < chunk_state->num_columns; i++)
	{
		DecompressChunkColumnState *column = &batch_state->columns[i];
		switch (column->type)
		{
			case COUNT_COLUMN:
				if (batch_state->counter <= 0)
					/*
					 * we continue checking other columns even if counter
					 * reaches zero to sanity check all columns are in sync
					 * and agree about batch end
					 */
					batch_done = true;
				else
					batch_state->counter--;
				break;
			case COMPRESSED_COLUMN:
			{
				AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);

				if (!column->compressed.iterator)
				{
					slot->tts_values[attr] = getmissingattr(slot->tts_tupleDescriptor,
															attr + 1,
															&slot->tts_isnull[attr]);
				}
				else
				{
					DecompressResult result;
					result = column->compressed.iterator->try_next(column->compressed.iterator);

					if (result.is_done)
					{
						batch_done = true;
						continue;
					}
					else if (batch_done)
					{
						/*
						 * since the count column is the first column batch_done
						 * might be true if compressed column is out of sync with
						 * the batch counter.
						 */
						elog(ERROR, "compressed column out of sync with batch counter");
					}

					slot->tts_values[attr] = result.val;
					slot->tts_isnull[attr] = result.is_null;
				}

				break;
			}
			case SEGMENTBY_COLUMN:
			{
				AttrNumber attr = AttrNumberGetAttrOffset(column->output_attno);

				slot->tts_values[attr] = column->segmentby.value;
				slot->tts_isnull[attr] = column->segmentby.isnull;
				break;
			}
			case SEQUENCE_NUM_COLUMN:
				/*
				 * nothing to do here for sequence number
				 * we only needed this for sorting in node below
				 */
				break;
		}
	}

	if (!batch_done)
		ExecStoreVirtualTuple(slot);
	else
		ExecClearTuple(slot);
}

/*
 * Create generated tuple according to column chunk_state
 */
static void
decompress_chunk_create_tuple(DecompressChunkState *chunk_state, DecompressBatchState *batch_state,
							  TupleTableSlot *slot)
{
	while (true)
	{
		if (!chunk_state->initialized)
		{
			TupleTableSlot *subslot = ExecProcNode(linitial(chunk_state->csstate.custom_ps));

			if (TupIsNull(subslot))
				return;

			initialize_batch(chunk_state, batch_state, subslot);
		}

		/* Decompress next tuple from batch */
		decompress_next_tuple_from_batch(chunk_state, batch_state, slot);

		if (!TupIsNull(slot))
			return;

		chunk_state->initialized = false;
	}
}
