/*
 * citus_meta_visibility.c
 *
 * Implements the functions for hiding citus relations.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"
#include "miscadmin.h"

#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "distributed/metadata_cache.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/citus_meta_visibility.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"


static Node * CreateCitusObjectFilter(int pgMetaTableVarno, int pgMetaTableOid);

PG_FUNCTION_INFO_V1(is_filtered_citus_object);

/*
 * is_filtered_citus_object a wrapper around IsFilteredCitusObject, so
 * see the details there.
 */
Datum
is_filtered_citus_object(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	Oid metaTableId = PG_GETARG_OID(1);

	PG_RETURN_BOOL(IsFilteredCitusObject(relationId, metaTableId));
}


/*
 * IsFilteredCitusObject gets an object oid and its meta table oid, 
 * and checks whether it's a filtered citus object.
 */
bool
IsFilteredCitusObject(Oid objectId, Oid metaTableId)
{
	if (!OidIsValid(objectId) || !OidIsValid(metaTableId))
	{
		/* we cannot continue without valid Oids */
		return false;
	}

	switch(metaTableId)
	{
		case RelationRelationId:
		{
			if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(objectId)))
			{
				/* relation does not exist */
				return false;
			}

			const char *citusRelations[] = {
				"pg_dist_authinfo", "pg_dist_node_metadata", "pg_dist_colocation", "pg_dist_local_group", 
				"pg_dist_partition", "pg_dist_placement", "pg_dist_rebalance_strategy", "pg_dist_shard", 
				"pg_dist_transaction", "pg_dist_authinfo_identification_index", "pg_dist_partition_logical_relid_index",
				"pg_dist_placement_placementid_index", "pg_dist_shard_shardid_index", "chunk", "chunk_group",
				"options", "pg_dist_node", "pg_dist_node_metadata", "pg_dist_object", "pg_dist_poolinfo", "stripe",
				"citus_dist_stat_activity", "citus_lock_waits", "citus_shard_indexes_on_worker", 
				"citus_shards", "citus_shards_on_worker", "citus_stat_activity", "citus_stat_statements",
				"citus_tables", "pg_dist_shard_placement", "time_partitions"
			};
			
			char *relationName = get_rel_name(objectId);

			for (int relNo = 0; relNo < lengthof(citusRelations); relNo++)
			{
				if(strcasecmp(citusRelations[relNo], relationName) == 0)
				{
					return true;
				}
			}

			break;
		}
		case ProcedureRelationId:
		{
			if (!SearchSysCacheExists1(PROCOID, ObjectIdGetDatum(objectId)))
			{
				/* proc does not exist */
				return false;
			}

			const char *citusProcs[] = {
				"coord_combine_agg_sfunc", "coord_combine_agg_ffunc", "coord_combine_agg", 
				"worker_partial_agg_ffunc", "worker_partial_agg", "array_cat_agg", "jsonb_cat_agg", 
				"json_cat_agg", "any_value", "worker_partial_agg", "coord_combine_agg", "is_filtered_citus_object"
			};

			/*
			 * Gets procedure signature and then extracts the name out of it.
			 */
			char *procSignature = format_procedure(objectId);
			char *argsIndex = strrchr(procSignature, '(');
			int procNameLen = argsIndex - procSignature;
			char *procName = palloc0(procNameLen + 1);
			memcpy(procName, procSignature, procNameLen);

			for (int procNo = 0; procNo < lengthof(citusProcs); procNo++)
			{
				if(strcasecmp(citusProcs[procNo], procName) == 0)
				{
					return true;
				}
			}

			break;
		}
		case AccessMethodRelationId:
		{
			if (!SearchSysCacheExists1(AMOID, ObjectIdGetDatum(objectId)))
			{
				/* access method does not exist */
				return false;
			}

			const char *citusAccessMethods[] = {
				"columnar"
			};
			
			char *amName = get_am_name(objectId);

			for (int amNo = 0; amNo < lengthof(citusAccessMethods); amNo++)
			{
				if(strcasecmp(citusAccessMethods[amNo], amName) == 0)
				{
					return true;
				}
			}

			break;
		}
		case TypeRelationId:
		{
			if (!SearchSysCacheExists1(TYPEOID, ObjectIdGetDatum(objectId)))
			{
				/* type does not exist */
				return false;
			}

			const char *citusTypes[] = {
				"citus.distribution_type", "citus.distribution_type[]", "noderole", "noderole[]",
				"citus.shard_transfer_mode", "citus.shard_transfer_mode[]", "citus_copy_format", "citus_copy_format[]"
			};
			
			char *typeName = format_type_be(objectId);

			for (int typeNo = 0; typeNo < lengthof(citusTypes); typeNo++)
			{
				if(strcasecmp(citusTypes[typeNo], typeName) == 0)
				{
					return true;
				}
			}

			break;
		}
		case EnumRelationId:
		{
			if (!SearchSysCacheExists1(ENUMOID, ObjectIdGetDatum(objectId)))
			{
				/* enum does not exist */
				return false;
			}

			const char *citusEnums[] = {
				"primary", "secondary", "noderole", "unavailable", "csv", "binary", "text",
				"hash", "append", "range", "auto", "force_logical", "block_writes"
			};
			
			HeapTuple enumTuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(
											  objectId));

			Form_pg_enum enumForm = (Form_pg_enum) GETSTRUCT(enumTuple);
			const char *enumLabel = pstrdup(NameStr(enumForm->enumlabel));

			ReleaseSysCache(enumTuple);

			for (int enumNo = 0; enumNo < lengthof(citusEnums); enumNo++)
			{
				if(strcasecmp(citusEnums[enumNo], enumLabel) == 0)
				{
					return true;
				}
			}

			break;
		}
		case EventTriggerRelationId:
		{
			if (!SearchSysCacheExists1(EVENTTRIGGEROID, ObjectIdGetDatum(objectId)))
			{
				/* event trigger does not exist */
				return false;
			}

			const char *citusEventTriggers[] = {
				"citus_cascade_to_partition"
			};
			
			HeapTuple eventTriggerTuple = SearchSysCache1(EVENTTRIGGEROID, ObjectIdGetDatum(
											  objectId));

			Form_pg_event_trigger eventTriggerForm = (Form_pg_event_trigger) GETSTRUCT(eventTriggerTuple);
			const char *eventTriggerName = pstrdup(NameStr(eventTriggerForm->evtname));

			ReleaseSysCache(eventTriggerTuple);

			for (int eventTriggerNo = 0; eventTriggerNo < lengthof(citusEventTriggers); eventTriggerNo++)
			{
				if(strcasecmp(citusEventTriggers[eventTriggerNo], eventTriggerName) == 0)
				{
					return true;
				}
			}

			break;
		}
		default:
		{
			break;
		}
	}

	return false;
}


/*
 * FilterCitusObjectsFromPgMetaTable adds a NOT is_filtered_citus_object(oid, oid) filter
 * to the security quals of pg_class RTEs.
 */
bool
FilterCitusObjectsFromPgMetaTable(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		MemoryContext queryContext = GetMemoryChunkContext(query);

		/*
		 * We process the whole rtable rather than visiting individual RangeTblEntry's
		 * in the walker, since we need to know the varno to generate the right
		 * filter.
		 */
		int varno = 0;
		RangeTblEntry *rangeTableEntry = NULL;

		foreach_ptr(rangeTableEntry, query->rtable)
		{
			varno++;

			if (rangeTableEntry->rtekind == RTE_RELATION)
			{
				/* make sure the expression is in the right memory context */
				MemoryContext originalContext = MemoryContextSwitchTo(queryContext);

				/* add NOT is_filtered_citus_object(oid, oid) to the security quals of the RTE */
				switch(rangeTableEntry->relid)
				{
					/* pg_class */
					case RelationRelationId:
					{
						rangeTableEntry->securityQuals = 
							list_make1(CreateCitusObjectFilter(varno, RelationRelationId));
						break;
					}
					/* pg_proc */
					case ProcedureRelationId:
					{
						rangeTableEntry->securityQuals = 
							list_make1(CreateCitusObjectFilter(varno, ProcedureRelationId));
						break;
					}
					/* pg_am */
					case AccessMethodRelationId:
					{
						rangeTableEntry->securityQuals = 
							list_make1(CreateCitusObjectFilter(varno, AccessMethodRelationId));
						break;
					}
					/* pg_type */
					case TypeRelationId:
					{
						rangeTableEntry->securityQuals = 
							list_make1(CreateCitusObjectFilter(varno, TypeRelationId));
						break;
					}
					/* pg_enum */
					case EnumRelationId:
					{
						rangeTableEntry->securityQuals = 
							list_make1(CreateCitusObjectFilter(varno, EnumRelationId));
						break;
					}
					/* pg_event_trigger */
					case EventTriggerRelationId:
					{
						rangeTableEntry->securityQuals = 
							list_make1(CreateCitusObjectFilter(varno, EventTriggerRelationId));
						break;
					}
					default:
					{
						break;
					}
				}

				MemoryContextSwitchTo(originalContext);
			}
		}

		return query_tree_walker((Query *) node, FilterCitusObjectsFromPgMetaTable, context, 0);
	}

	return expression_tree_walker(node, FilterCitusObjectsFromPgMetaTable, context);
}


/*
 * CreateCitusObjectFilter constructs an expression of the form:
 * NOT pg_catalog.is_filtered_citus_object(oid, oid)
 */
static Node *
CreateCitusObjectFilter(int pgMetaTableVarno, int pgMetaTableOid)
{
	/* oid is always the first column */
	AttrNumber oidAttNum = 1;

	Var *oidVar = makeVar(pgMetaTableVarno, oidAttNum, OIDOID, -1, InvalidOid, 0);
	Const *oidConst = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
										   ObjectIdGetDatum(pgMetaTableOid),
										   false, true);

	/* build the call to read_intermediate_result */
	FuncExpr *funcExpr = makeNode(FuncExpr);
	funcExpr->funcid = FilteredCitusObjectFuncId();
	funcExpr->funcretset = false;
	funcExpr->funcvariadic = false;
	funcExpr->funcformat = 0;
	funcExpr->funccollid = 0;
	funcExpr->inputcollid = 0;
	funcExpr->location = -1;
	funcExpr->args = list_make2((Node *)oidVar, (Node *)oidConst);

	BoolExpr *notExpr = makeNode(BoolExpr);
	notExpr->boolop = NOT_EXPR;
	notExpr->args = list_make1(funcExpr);
	notExpr->location = -1;

	return (Node *) notExpr;
}
