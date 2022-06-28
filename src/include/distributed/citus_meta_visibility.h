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
extern bool IsFilteredCitusObject(Oid relationId, Oid pgMetaTableOid);


#endif /* CITUS_META_VISIBILITY_H */
