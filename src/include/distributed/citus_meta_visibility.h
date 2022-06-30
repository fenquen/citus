/*-------------------------------------------------------------------------
 *
 * citus_meta_visibility.h
 *   Hide citus relations.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_META_VISIBILITY_H
#define CITUS_META_VISIBILITY_H

#include "nodes/nodes.h"

extern bool FilterCitusObjectsFromPgMetaTable(Node *node, void *context);
extern bool IsFilteredCitusRelation(Oid relationId);
extern bool IsFilteredCitusProc(Oid procId);
extern bool IsFilteredCitusAm(Oid amId);
extern bool IsFilteredCitusType(Oid typeId);
extern bool IsFilteredCitusEnum(Oid enumId);
extern bool IsFilteredCitusEventTrigger(Oid eventTriggerId);
extern bool IsFilteredCitusTrigger(Oid triggerId);
extern bool IsFilteredCitusIndex(Oid indexId);
extern bool IsFilteredCitusAggregate(Oid aggregateId);
extern bool IsFilteredCitusRewrite(Oid rewriteRelationId);
extern bool IsFilteredCitusAttrDefault(Oid attributeDefaultRelationId);
extern bool IsFilteredCitusConstraint(Oid constraintId);
extern bool IsFilteredCitusAttribute(Oid attributeRelationId, short attNum);

#endif /* CITUS_META_VISIBILITY_H */
