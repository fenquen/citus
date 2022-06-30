/*
 * citus_meta_visibility.c
 *
 * Implements the functions for hiding citus relations.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"
#include "miscadmin.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_class.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "distributed/commands.h"
#include "distributed/metadata_cache.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/citus_meta_visibility.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static Node * CreateCitusObjectFilter(int pgMetaTableVarno, int pgMetaTableOid);
static List * GetFuncArgs(int pgMetaTableVarno, int pgMetaTableOid);

PG_FUNCTION_INFO_V1(is_filtered_citus_object);

/*
 * is_filtered_citus_object a wrapper around IsFilteredCitusObject, so
 * see the details there.
 */
Datum
is_filtered_citus_object(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid metaTableId = PG_GETARG_OID(0);
	if (!OidIsValid(metaTableId))
	{
		/* we cannot continue without valid meta table oid */
		PG_RETURN_BOOL(false);
	}

	bool isFiltered = false;

	Oid objectId = PG_GETARG_OID(1);
	switch (metaTableId)
	{
		case RelationRelationId:
		{
			isFiltered = IsFilteredCitusRelation(objectId);
			break;
		}

		case ProcedureRelationId:
		{
			isFiltered = IsFilteredCitusProc(objectId);
			break;
		}

		case AccessMethodRelationId:
		{
			isFiltered = IsFilteredCitusAm(objectId);
			break;
		}

		case TypeRelationId:
		{
			isFiltered = IsFilteredCitusType(objectId);
			break;
		}

		case EnumRelationId:
		{
			isFiltered = IsFilteredCitusEnum(objectId);
			break;
		}

		case EventTriggerRelationId:
		{
			isFiltered = IsFilteredCitusEventTrigger(objectId);
			break;
		}

		case TriggerRelationId:
		{
			isFiltered = IsFilteredCitusTrigger(objectId);
			break;
		}

		case IndexRelationId:
		{
			isFiltered = IsFilteredCitusIndex(objectId);
			break;
		}

		case AggregateRelationId:
		{
			isFiltered = IsFilteredCitusAggregate(objectId);
			break;
		}

		case RewriteRelationId:
		{
			isFiltered = IsFilteredCitusRewrite(objectId);
			break;
		}

		case AttrDefaultRelationId:
		{
			isFiltered = IsFilteredCitusAttrDefault(objectId);
			break;
		}

		case ConstraintRelationId:
		{
			isFiltered = IsFilteredCitusConstraint(objectId);
			break;
		}

		case AttributeRelationId:
		{
			int16 attNum = PG_GETARG_INT16(2);
			isFiltered = IsFilteredCitusAttribute(objectId, attNum);
			break;
		}

		default:
		{
			break;
		}
	}

	PG_RETURN_BOOL(isFiltered);
}


/*
 * IsFilteredCitusRelation returns true if given relation is a filtered citus relation.
 */
bool
IsFilteredCitusRelation(Oid relationId)
{
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)))
	{
		/* relation does not exist */
		return false;
	}

	const char *citusRelations[] = {
		"pg_dist_authinfo", "pg_dist_node_metadata", "pg_dist_colocation",
		"pg_dist_local_group",
		"pg_dist_partition", "pg_dist_placement", "pg_dist_rebalance_strategy",
		"pg_dist_shard",
		"pg_dist_transaction", "pg_dist_authinfo_identification_index",
		"pg_dist_partition_logical_relid_index",
		"pg_dist_placement_placementid_index", "pg_dist_shard_shardid_index", "chunk",
		"chunk_group",
		"options", "pg_dist_node", "pg_dist_node_metadata", "pg_dist_object",
		"pg_dist_poolinfo", "stripe",
		"citus_dist_stat_activity", "citus_lock_waits", "citus_shard_indexes_on_worker",
		"citus_shards", "citus_shards_on_worker", "citus_stat_activity",
		"citus_stat_statements",
		"citus_tables", "pg_dist_shard_placement", "time_partitions"
	};

	char *relationName = get_rel_name(relationId);

	for (int relNo = 0; relNo < lengthof(citusRelations); relNo++)
	{
		if (strcasecmp(citusRelations[relNo], relationName) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * IsFilteredCitusProc returns true if given proc is a filtered citus proc.
 */
bool
IsFilteredCitusProc(Oid procId)
{
	HeapTuple procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(
											  procId));
	if (!HeapTupleIsValid(procTuple))
	{
		/* proc does not exist */
		return false;
	}

	Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(procTuple);
	char *procName = pstrdup(NameStr(procForm->proname));

	bool foundFilteredType = false;
	int argIdx;
	for (argIdx = 0; argIdx < procForm->pronargs; argIdx++)
	{
		if (IsFilteredCitusType(procForm->proargtypes.values[argIdx]))
		{
			foundFilteredType = true;
			break;
		}
	}

	ReleaseSysCache(procTuple);

	if (foundFilteredType)
	{
		return true;
	}

	const char *citusProcs[] = {
		"coord_combine_agg_sfunc", "coord_combine_agg_ffunc", "coord_combine_agg",
		"worker_partial_agg_ffunc", "worker_partial_agg", "array_cat_agg",
		"jsonb_cat_agg", "json_cat_agg", "any_value", "worker_partial_agg",
		"coord_combine_agg", "is_filtered_citus_object", "citus_drop_trigger",
		"master_move_shard_placement", "master_copy_shard_placement",
		"replicate_table_shards",
		"master_drain_node", "rebalance_table_shards", "citus_drain_node",
		"citus_copy_shard_placement",
		"citus_move_shard_placement", "read_intermediate_result",
		"read_intermediate_results",
		"master_add_node", "master_add_inactive_node", "citus_set_coordinator_host",
		"citus_add_node",
		"citus_add_inactive_node", "create_distributed_table",
		"worker_partition_query_result"
	};

	for (int procNo = 0; procNo < lengthof(citusProcs); procNo++)
	{
		if (strcasecmp(citusProcs[procNo], procName) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * IsFilteredCitusAm returns true if given am is a filtered citus am.
 */
bool
IsFilteredCitusAm(Oid amId)
{
	if (!SearchSysCacheExists1(AMOID, ObjectIdGetDatum(amId)))
	{
		/* access method does not exist */
		return false;
	}

	const char *citusAccessMethods[] = {
		"columnar"
	};

	char *amName = get_am_name(amId);

	for (int amNo = 0; amNo < lengthof(citusAccessMethods); amNo++)
	{
		if (strcasecmp(citusAccessMethods[amNo], amName) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * IsFilteredCitusType returns true if given type is a filtered citus type.
 */
bool
IsFilteredCitusType(Oid typeId)
{
	HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(
											  typeId));
	if (!HeapTupleIsValid(typeTuple))
	{
		/* type does not exist */
		return false;
	}

	Form_pg_type typeForm = (Form_pg_type) GETSTRUCT(typeTuple);
	Oid typeRelationId = typeForm->typrelid;
	Oid typeElemId = typeForm->typelem;

	ReleaseSysCache(typeTuple);

	const char *citusTypes[] = {
		"citus.distribution_type", "noderole", "citus.shard_transfer_mode",
		"citus_copy_format"
	};

	char *typeName = format_type_be(typeId);

	for (int typeNo = 0; typeNo < lengthof(citusTypes); typeNo++)
	{
		if (strcasecmp(citusTypes[typeNo], typeName) == 0)
		{
			return true;
		}
	}

	return IsFilteredCitusRelation(typeRelationId) || IsFilteredCitusType(typeElemId);
}


/*
 * IsFilteredCitusEnum returns true if given enum is a filtered citus enum.
 */
bool
IsFilteredCitusEnum(Oid enumId)
{
	HeapTuple enumTuple = SearchSysCache1(ENUMOID, ObjectIdGetDatum(
											  enumId));
	if (!HeapTupleIsValid(enumTuple))
	{
		/* enum does not exist */
		return false;
	}

	Form_pg_enum enumForm = (Form_pg_enum) GETSTRUCT(enumTuple);
	Oid enumTypeId = enumForm->enumtypid;

	ReleaseSysCache(enumTuple);

	return IsFilteredCitusType(enumTypeId);
}


/*
 * IsFilteredCitusEventTrigger returns true if given event trigger is a filtered citus event trigger.
 */
bool
IsFilteredCitusEventTrigger(Oid eventTriggerId)
{
	HeapTuple eventTriggerTuple = SearchSysCache1(EVENTTRIGGEROID, ObjectIdGetDatum(
													  eventTriggerId));
	if (!HeapTupleIsValid(eventTriggerTuple))
	{
		/* event trigger does not exist */
		return false;
	}

	Form_pg_event_trigger eventTriggerForm = (Form_pg_event_trigger) GETSTRUCT(
		eventTriggerTuple);
	Oid eventTriggerFuncId = eventTriggerForm->evtfoid;

	ReleaseSysCache(eventTriggerTuple);

	return IsFilteredCitusProc(eventTriggerFuncId);
}


/*
 * IsFilteredCitusTrigger returns true if given trigger is a filtered citus trigger.
 */
bool
IsFilteredCitusTrigger(Oid triggerId)
{
	bool missingOk = true;
	HeapTuple triggerTuple = GetTriggerTupleById(triggerId, missingOk);
	if (!HeapTupleIsValid(triggerTuple))
	{
		/* trigger does not exist */
		return false;
	}

	Form_pg_trigger triggerForm = (Form_pg_trigger) GETSTRUCT(triggerTuple);
	Oid triggerRelationId = triggerForm->tgrelid;
	Oid triggerParentTriggerId = triggerForm->tgparentid;
	Oid triggerFuncId = triggerForm->tgfoid;

	return IsFilteredCitusRelation(triggerRelationId) ||
		   IsFilteredCitusTrigger(triggerParentTriggerId) ||
		   IsFilteredCitusProc(triggerFuncId);
}


/*
 * IsFilteredCitusIndex returns true if given index is a filtered citus index.
 */
bool
IsFilteredCitusIndex(Oid indexId)
{
	HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
	if (!HeapTupleIsValid(indexTuple))
	{
		/* index not found */
		return false;
	}

	Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	Oid indexRelationId = indexForm->indrelid;

	ReleaseSysCache(indexTuple);

	return IsFilteredCitusRelation(indexRelationId);
}


/*
 * IsFilteredCitusAggregate returns true if given aggregate is a filtered citus aggregate.
 */
bool
IsFilteredCitusAggregate(Oid aggregateId)
{
	HeapTuple aggregateTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggregateId));
	if (!HeapTupleIsValid(aggregateTuple))
	{
		/* aggregate not found */
		return false;
	}

	Form_pg_aggregate aggregateForm = (Form_pg_aggregate) GETSTRUCT(aggregateTuple);
	Oid transFuncId = aggregateForm->aggtransfn;
	Oid finalFuncId = aggregateForm->aggfinalfn;
	Oid combineFuncId = aggregateForm->aggcombinefn;
	Oid serialFuncId = aggregateForm->aggserialfn;
	Oid deserialFuncId = aggregateForm->aggdeserialfn;
	Oid mtransFuncId = aggregateForm->aggmtransfn;
	Oid minvtransFuncId = aggregateForm->aggminvtransfn;
	Oid mfinalFuncId = aggregateForm->aggmfinalfn;
	Oid transTypeId = aggregateForm->aggtranstype;
	Oid mtransTypeId = aggregateForm->aggmtranstype;

	ReleaseSysCache(aggregateTuple);

	return IsFilteredCitusProc(aggregateId) || IsFilteredCitusProc(transFuncId) ||
		   IsFilteredCitusProc(finalFuncId) || IsFilteredCitusProc(combineFuncId) ||
		   IsFilteredCitusProc(serialFuncId) || IsFilteredCitusProc(deserialFuncId) ||
		   IsFilteredCitusProc(mtransFuncId) || IsFilteredCitusProc(minvtransFuncId) ||
		   IsFilteredCitusProc(mfinalFuncId) || IsFilteredCitusType(transTypeId) ||
		   IsFilteredCitusType(mtransTypeId);
}


/*
 * IsFilteredCitusRewrite returns true if given rewrite is a filtered citus rewrite.
 */
bool
IsFilteredCitusRewrite(Oid rewriteRelationId)
{
	return IsFilteredCitusRelation(rewriteRelationId);
}


/*
 * IsFilteredCitusAttrDefault returns true if given attribute default is a filtered citus attribute default.
 */
bool
IsFilteredCitusAttrDefault(Oid attributeDefaultRelationId)
{
	return IsFilteredCitusRelation(attributeDefaultRelationId);
}


/*
 * IsFilteredCitusConstraint returns true if given constraint is a filtered citus constraint.
 */
bool
IsFilteredCitusConstraint(Oid constraintId)
{
	HeapTuple constraintTuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(
													constraintId));
	if (!HeapTupleIsValid(constraintTuple))
	{
		/* constraint not found */
		return false;
	}

	Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(constraintTuple);
	Oid constraintRelationId = constraintForm->conrelid;
	Oid constraintForeignRelationId = constraintForm->confrelid;
	Oid constraintIndexId = constraintForm->conindid;
	Oid constraintParentId = constraintForm->conparentid;

	ReleaseSysCache(constraintTuple);

	return IsFilteredCitusRelation(constraintRelationId) ||
		   IsFilteredCitusRelation(constraintForeignRelationId) ||
		   IsFilteredCitusIndex(constraintIndexId) ||
		   IsFilteredCitusConstraint(constraintParentId);
}


/*
 * IsFilteredCitusAttribute returns true if given attribute is a filtered citus attribute.
 */
bool
IsFilteredCitusAttribute(Oid attributeRelationId, short attNum)
{
	HeapTuple attributeTuple = SearchSysCache2(ATTNUM,
											   ObjectIdGetDatum(attributeRelationId),
											   Int16GetDatum(attNum));
	if (!HeapTupleIsValid(attributeTuple))
	{
		/* attribute does not exist */
		return false;
	}

	Form_pg_attribute attributeForm = (Form_pg_attribute) GETSTRUCT(attributeTuple);
	Oid attributeTypeId = attributeForm->atttypid;

	ReleaseSysCache(attributeTuple);

	return IsFilteredCitusRelation(attributeRelationId) || IsFilteredCitusType(
		attributeTypeId);
}


/*
 * FilterCitusObjectsFromPgMetaTable adds a NOT is_filtered_citus_object(oid, oid, smallint) filter
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

				/* add NOT is_filtered_citus_object(oid, oid, smallint) to the security quals of the RTE */
				switch (rangeTableEntry->relid)
				{
					/* pg_class */
					case RelationRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno,
															   RelationRelationId));
						break;
					}

					/* pg_proc */
					case ProcedureRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno,
															   ProcedureRelationId));
						break;
					}

					/* pg_am */
					case AccessMethodRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno,
															   AccessMethodRelationId));
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
							list_make1(CreateCitusObjectFilter(varno,
															   EventTriggerRelationId));
						break;
					}

					/* pg_trigger */
					case TriggerRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno, TriggerRelationId));
						break;
					}

					/* pg_index */
					case IndexRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno, IndexRelationId));
						break;
					}

					/* pg_aggregate */
					case AggregateRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno,
															   AggregateRelationId));
						break;
					}

					/* pg_rewrite */
					case RewriteRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno,
															   RewriteRelationId));
						break;
					}

					/* pg_attrdef */
					case AttrDefaultRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno,
															   AttrDefaultRelationId));
						break;
					}

					/* pg_constraint */
					case ConstraintRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno,
															   ConstraintRelationId));
						break;
					}

					/* pg_attribute */
					case AttributeRelationId:
					{
						rangeTableEntry->securityQuals =
							list_make1(CreateCitusObjectFilter(varno,
															   AttributeRelationId));
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

		return query_tree_walker((Query *) node, FilterCitusObjectsFromPgMetaTable,
								 context, 0);
	}

	return expression_tree_walker(node, FilterCitusObjectsFromPgMetaTable, context);
}


/*
 * CreateCitusObjectFilter constructs an expression of the form:
 * NOT pg_catalog.is_filtered_citus_object(oid, oid, smallint)
 */
static Node *
CreateCitusObjectFilter(int pgMetaTableVarno, int pgMetaTableOid)
{
	/* build the call to read_intermediate_result */
	FuncExpr *funcExpr = makeNode(FuncExpr);
	funcExpr->funcid = FilteredCitusObjectFuncId();
	funcExpr->funcretset = false;
	funcExpr->funcvariadic = false;
	funcExpr->funcformat = 0;
	funcExpr->funccollid = 0;
	funcExpr->inputcollid = 0;
	funcExpr->location = -1;
	funcExpr->args = GetFuncArgs(pgMetaTableVarno, pgMetaTableOid);

	BoolExpr *notExpr = makeNode(BoolExpr);
	notExpr->boolop = NOT_EXPR;
	notExpr->args = list_make1(funcExpr);
	notExpr->location = -1;

	return (Node *) notExpr;
}


/*
 * GetFuncArgs returns func arguments for pg_catalog.is_filtered_citus_object
 */
static List *
GetFuncArgs(int pgMetaTableVarno, int pgMetaTableOid)
{
	Const *metaTableOidConst = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
										 ObjectIdGetDatum(pgMetaTableOid),
										 false, true);

	AttrNumber oidAttNum = 1;
	if (pgMetaTableOid == RewriteRelationId)
	{
		oidAttNum = 3;
	}
	else if (pgMetaTableOid == AttrDefaultRelationId)
	{
		oidAttNum = 2;
	}

	Var *oidVar = makeVar(pgMetaTableVarno, oidAttNum,
						  (pgMetaTableOid == AggregateRelationId) ? REGPROCOID : OIDOID,
						  -1,
						  InvalidOid, 0);

	if (pgMetaTableOid == AttributeRelationId)
	{
		AttrNumber attnumAttNum = 6;
		Var *attnumVar = makeVar(pgMetaTableVarno, attnumAttNum, INT2OID, -1, InvalidOid,
								 0);

		return list_make3((Node *) metaTableOidConst, (Node *) oidVar,
						  (Node *) attnumVar);
	}
	else
	{
		Const *dummyAttnumConst = makeConst(INT2OID, -1, InvalidOid, sizeof(int16),
											Int16GetDatum(0),
											false, true);

		return list_make3((Node *) metaTableOidConst, (Node *) oidVar,
						  (Node *) dummyAttnumConst);
	}
}
