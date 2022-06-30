/*-------------------------------------------------------------------------
 *
 * distribute_object_ops.c
 *
 *    Contains declarations for DistributeObjectOps, along with their
 *    lookup function, GetDistributeObjectOps.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/pg_version_constants.h"
#include "distributed/version_compat.h"
#include "distributed/commands/utility_hook.h"

static DistributeObjectOps NoDistributeOps = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Aggregate_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_FUNCTION,
	.address = AlterFunctionSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Aggregate_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_FUNCTION,
	.address = AlterFunctionOwnerObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Aggregate_Define = {
	.deparse = NULL,
	.qualify = QualifyDefineAggregateStmt,
	.preprocess = NULL,
	.postprocess = PostprocessCreateDistributedObjectFromCatalogStmt,
	.objectType = OBJECT_AGGREGATE,
	.address = DefineAggregateStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Aggregate_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Aggregate_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_FUNCTION,
	.address = RenameFunctionStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Any_AlterEnum = {
	.deparse = DeparseAlterEnumStmt,
	.qualify = QualifyAlterEnumStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_TYPE,
	.address = AlterEnumStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Any_AlterExtension = {
	.deparse = DeparseAlterExtensionStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterExtensionUpdateStmt,
	.postprocess = NULL,
	.address = AlterExtensionUpdateStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Any_AlterExtensionContents = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterExtensionContentsStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_AlterForeignServer = {
	.deparse = DeparseAlterForeignServerStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_FOREIGN_SERVER,
	.address = AlterForeignServerStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Any_AlterFunction = {
	.deparse = DeparseAlterFunctionStmt,
	.qualify = QualifyAlterFunctionStmt,
	.preprocess = PreprocessAlterFunctionStmt,
	.postprocess = NULL,
	.address = AlterFunctionStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Any_AlterPolicy = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterPolicyStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_AlterRole = {
	.deparse = DeparseAlterRoleStmt,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessAlterRoleStmt,
	.address = AlterRoleStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Any_AlterRoleSet = {
	.deparse = DeparseAlterRoleSetStmt,
	.qualify = QualifyAlterRoleSetStmt,
	.preprocess = PreprocessAlterRoleSetStmt,
	.postprocess = NULL,
	.address = AlterRoleSetStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Any_AlterTableMoveAll = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableMoveAllStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_Cluster = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessClusterStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_CompositeType = {
	.deparse = DeparseCompositeTypeStmt,
	.qualify = QualifyCompositeTypeStmt,
	.preprocess = NULL,
	.postprocess = PostprocessCreateDistributedObjectFromCatalogStmt,
	.objectType = OBJECT_TYPE,
	.featureFlag = &EnableCreateTypePropagation,
	.address = CompositeTypeStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Any_CreateDomain = {
	.deparse = DeparseCreateDomainStmt,
	.qualify = QualifyCreateDomainStmt,
	.preprocess = NULL,
	.postprocess = PostprocessCreateDistributedObjectFromCatalogStmt,
	.objectType = OBJECT_DOMAIN,
	.address = CreateDomainStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Any_CreateEnum = {
	.deparse = DeparseCreateEnumStmt,
	.qualify = QualifyCreateEnumStmt,
	.preprocess = NULL,
	.postprocess = PostprocessCreateDistributedObjectFromCatalogStmt,
	.objectType = OBJECT_TYPE,
	.featureFlag = &EnableCreateTypePropagation,
	.address = CreateEnumStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Any_CreateExtension = {
	.deparse = DeparseCreateExtensionStmt,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessCreateExtensionStmt,
	.address = CreateExtensionStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Any_CreateFunction = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessCreateFunctionStmt,
	.postprocess = PostprocessCreateFunctionStmt,
	.address = CreateFunctionStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Any_View = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessViewStmt,
	.postprocess = PostprocessViewStmt,
	.address = ViewStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Any_CreatePolicy = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessCreatePolicyStmt,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_CreateRole = {
	.deparse = DeparseCreateRoleStmt,
	.qualify = NULL,
	.preprocess = PreprocessCreateRoleStmt,
	.postprocess = NULL,
	.address = CreateRoleStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Any_DropRole = {
	.deparse = DeparseDropRoleStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropRoleStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_CreateForeignServer = {
	.deparse = DeparseCreateForeignServerStmt,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessCreateDistributedObjectFromCatalogStmt,
	.objectType = OBJECT_FOREIGN_SERVER,
	.address = CreateForeignServerStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Any_CreateSchema = {
	.deparse = DeparseCreateSchemaStmt,
	.qualify = NULL,
	.preprocess = PreprocessCreateSchemaStmt,
	.postprocess = NULL,
	.address = CreateSchemaStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Any_CreateStatistics = {
	.deparse = DeparseCreateStatisticsStmt,
	.qualify = QualifyCreateStatisticsStmt,
	.preprocess = PreprocessCreateStatisticsStmt,
	.postprocess = PostprocessCreateStatisticsStmt,
	.address = CreateStatisticsStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Any_CreateTrigger = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessCreateTriggerStmt,
	.address = CreateTriggerStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Any_Grant = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessGrantStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_GrantRole = {
	.deparse = DeparseGrantRoleStmt,
	.qualify = NULL,
	.preprocess = PreprocessGrantRoleStmt,
	.postprocess = PostprocessGrantRoleStmt,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_Index = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessIndexStmt,
	.postprocess = PostprocessIndexStmt,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_Reindex = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessReindexStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Any_Rename = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessRenameStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Attribute_Rename = {
	.deparse = DeparseRenameAttributeStmt,
	.qualify = QualifyRenameAttributeStmt,
	.preprocess = PreprocessRenameAttributeStmt,
	.postprocess = NULL,
	.address = RenameAttributeStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Collation_AlterObjectSchema = {
	.deparse = DeparseAlterCollationSchemaStmt,
	.qualify = QualifyAlterCollationSchemaStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_COLLATION,
	.address = AlterCollationSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Collation_AlterOwner = {
	.deparse = DeparseAlterCollationOwnerStmt,
	.qualify = QualifyAlterCollationOwnerStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_COLLATION,
	.address = AlterCollationOwnerObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Collation_Define = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessCreateDistributedObjectFromCatalogStmt,
	.objectType = OBJECT_COLLATION,
	.address = DefineCollationStmtObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps Collation_Drop = {
	.deparse = DeparseDropCollationStmt,
	.qualify = QualifyDropCollationStmt,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Collation_Rename = {
	.deparse = DeparseRenameCollationStmt,
	.qualify = QualifyRenameCollationStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_COLLATION,
	.address = RenameCollationStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Database_AlterOwner = {
	.deparse = DeparseAlterDatabaseOwnerStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_DATABASE,
	.featureFlag = &EnableAlterDatabaseOwner,
	.address = AlterDatabaseOwnerObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Domain_Alter = {
	.deparse = DeparseAlterDomainStmt,
	.qualify = QualifyAlterDomainStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_DOMAIN,
	.address = AlterDomainStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Domain_AlterObjectSchema = {
	.deparse = DeparseAlterDomainSchemaStmt,
	.qualify = QualifyAlterDomainSchemaStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_DOMAIN,
	.address = AlterTypeSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Domain_AlterOwner = {
	.deparse = DeparseAlterDomainOwnerStmt,
	.qualify = QualifyAlterDomainOwnerStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_DOMAIN,
	.address = AlterDomainOwnerStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Domain_Drop = {
	.deparse = DeparseDropDomainStmt,
	.qualify = QualifyDropDomainStmt,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Domain_Rename = {
	.deparse = DeparseRenameDomainStmt,
	.qualify = QualifyRenameDomainStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_DOMAIN,
	.address = RenameDomainStmtObjectAddress,
	.markDistributed = false,
};

static DistributeObjectOps Domain_RenameConstraint = {
	.deparse = DeparseDomainRenameConstraintStmt,
	.qualify = QualifyDomainRenameConstraintStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_DOMAIN,
	.address = DomainRenameConstraintStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Extension_AlterObjectSchema = {
	.deparse = DeparseAlterExtensionSchemaStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterExtensionSchemaStmt,
	.postprocess = PostprocessAlterExtensionSchemaStmt,
	.address = AlterExtensionSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Extension_Drop = {
	.deparse = DeparseDropExtensionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropExtensionStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps FDW_Grant = {
	.deparse = DeparseGrantOnFDWStmt,
	.qualify = NULL,
	.preprocess = PreprocessGrantOnFDWStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps ForeignServer_Drop = {
	.deparse = DeparseDropForeignServerStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps ForeignServer_Grant = {
	.deparse = DeparseGrantOnForeignServerStmt,
	.qualify = NULL,
	.preprocess = PreprocessGrantOnForeignServerStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps ForeignServer_Rename = {
	.deparse = DeparseAlterForeignServerRenameStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_FOREIGN_SERVER,
	.address = RenameForeignServerStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps ForeignServer_AlterOwner = {
	.deparse = DeparseAlterForeignServerOwnerStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_FOREIGN_SERVER,
	.address = AlterForeignServerOwnerStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps ForeignTable_AlterTable = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Function_AlterObjectDepends = {
	.deparse = DeparseAlterFunctionDependsStmt,
	.qualify = QualifyAlterFunctionDependsStmt,
	.preprocess = PreprocessAlterFunctionDependsStmt,
	.postprocess = NULL,
	.address = AlterFunctionDependsStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Function_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_FUNCTION,
	.address = AlterFunctionSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Function_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_FUNCTION,
	.address = AlterFunctionOwnerObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Function_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Function_Grant = {
	.deparse = DeparseGrantOnFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessGrantOnFunctionStmt,
	.postprocess = PostprocessGrantOnFunctionStmt,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps View_Drop = {
	.deparse = DeparseDropViewStmt,
	.qualify = QualifyDropViewStmt,
	.preprocess = PreprocessDropViewStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Function_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_FUNCTION,
	.address = RenameFunctionStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Index_AlterTable = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Index_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropIndexStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Policy_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropPolicyStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Procedure_AlterObjectDepends = {
	.deparse = DeparseAlterFunctionDependsStmt,
	.qualify = QualifyAlterFunctionDependsStmt,
	.preprocess = PreprocessAlterFunctionDependsStmt,
	.postprocess = NULL,
	.address = AlterFunctionDependsStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Procedure_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_FUNCTION,
	.address = AlterFunctionSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Procedure_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_FUNCTION,
	.address = AlterFunctionOwnerObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Procedure_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Procedure_Grant = {
	.deparse = DeparseGrantOnFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessGrantOnFunctionStmt,
	.postprocess = PostprocessGrantOnFunctionStmt,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Procedure_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_FUNCTION,
	.address = RenameFunctionStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Routine_AlterObjectDepends = {
	.deparse = DeparseAlterFunctionDependsStmt,
	.qualify = QualifyAlterFunctionDependsStmt,
	.preprocess = PreprocessAlterFunctionDependsStmt,
	.postprocess = NULL,
	.address = AlterFunctionDependsStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Sequence_Alter = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterSequenceStmt,
	.postprocess = NULL,
	.address = AlterSequenceStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Sequence_AlterObjectSchema = {
	.deparse = DeparseAlterSequenceSchemaStmt,
	.qualify = QualifyAlterSequenceSchemaStmt,
	.preprocess = PreprocessAlterSequenceSchemaStmt,
	.postprocess = PostprocessAlterSequenceSchemaStmt,
	.address = AlterSequenceSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Sequence_AlterOwner = {
	.deparse = DeparseAlterSequenceOwnerStmt,
	.qualify = QualifyAlterSequenceOwnerStmt,
	.preprocess = PreprocessAlterSequenceOwnerStmt,
	.postprocess = PostprocessAlterSequenceOwnerStmt,
	.address = AlterSequenceOwnerStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Sequence_Drop = {
	.deparse = DeparseDropSequenceStmt,
	.qualify = QualifyDropSequenceStmt,
	.preprocess = PreprocessDropSequenceStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Sequence_Grant = {
	.deparse = DeparseGrantOnSequenceStmt,
	.qualify = QualifyGrantOnSequenceStmt,
	.preprocess = PreprocessGrantOnSequenceStmt,
	.postprocess = PostprocessGrantOnSequenceStmt,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Sequence_Rename = {
	.deparse = DeparseRenameSequenceStmt,
	.qualify = QualifyRenameSequenceStmt,
	.preprocess = PreprocessRenameSequenceStmt,
	.postprocess = NULL,
	.address = RenameSequenceStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchConfig_Alter = {
	.deparse = DeparseAlterTextSearchConfigurationStmt,
	.qualify = QualifyAlterTextSearchConfigurationStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_TSCONFIGURATION,
	.address = AlterTextSearchConfigurationStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchConfig_AlterObjectSchema = {
	.deparse = DeparseAlterTextSearchConfigurationSchemaStmt,
	.qualify = QualifyAlterTextSearchConfigurationSchemaStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_TSCONFIGURATION,
	.address = AlterTextSearchConfigurationSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchConfig_AlterOwner = {
	.deparse = DeparseAlterTextSearchConfigurationOwnerStmt,
	.qualify = QualifyAlterTextSearchConfigurationOwnerStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_TSCONFIGURATION,
	.address = AlterTextSearchConfigurationOwnerObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchConfig_Comment = {
	.deparse = DeparseTextSearchConfigurationCommentStmt,
	.qualify = QualifyTextSearchConfigurationCommentStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_TSCONFIGURATION,
	.address = TextSearchConfigurationCommentObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchConfig_Define = {
	.deparse = DeparseCreateTextSearchConfigurationStmt,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessCreateDistributedObjectFromCatalogStmt,
	.objectType = OBJECT_TSCONFIGURATION,
	.address = CreateTextSearchConfigurationObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps TextSearchConfig_Drop = {
	.deparse = DeparseDropTextSearchConfigurationStmt,
	.qualify = QualifyDropTextSearchConfigurationStmt,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchConfig_Rename = {
	.deparse = DeparseRenameTextSearchConfigurationStmt,
	.qualify = QualifyRenameTextSearchConfigurationStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_TSCONFIGURATION,
	.address = RenameTextSearchConfigurationStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchDict_Alter = {
	.deparse = DeparseAlterTextSearchDictionaryStmt,
	.qualify = QualifyAlterTextSearchDictionaryStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_TSDICTIONARY,
	.address = AlterTextSearchDictionaryStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchDict_AlterObjectSchema = {
	.deparse = DeparseAlterTextSearchDictionarySchemaStmt,
	.qualify = QualifyAlterTextSearchDictionarySchemaStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_TSDICTIONARY,
	.address = AlterTextSearchDictionarySchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchDict_AlterOwner = {
	.deparse = DeparseAlterTextSearchDictionaryOwnerStmt,
	.qualify = QualifyAlterTextSearchDictionaryOwnerStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_TSDICTIONARY,
	.address = AlterTextSearchDictOwnerObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchDict_Comment = {
	.deparse = DeparseTextSearchDictionaryCommentStmt,
	.qualify = QualifyTextSearchDictionaryCommentStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_TSDICTIONARY,
	.address = TextSearchDictCommentObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchDict_Define = {
	.deparse = DeparseCreateTextSearchDictionaryStmt,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessCreateDistributedObjectFromCatalogStmt,
	.objectType = OBJECT_TSDICTIONARY,
	.address = CreateTextSearchDictObjectAddress,
	.markDistributed = true,
};
static DistributeObjectOps TextSearchDict_Drop = {
	.deparse = DeparseDropTextSearchDictionaryStmt,
	.qualify = QualifyDropTextSearchDictionaryStmt,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps TextSearchDict_Rename = {
	.deparse = DeparseRenameTextSearchDictionaryStmt,
	.qualify = QualifyRenameTextSearchDictionaryStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_TSDICTIONARY,
	.address = RenameTextSearchDictionaryStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Trigger_AlterObjectDepends = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessAlterTriggerDependsStmt,
	.postprocess = PostprocessAlterTriggerDependsStmt,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Routine_AlterObjectSchema = {
	.deparse = DeparseAlterFunctionSchemaStmt,
	.qualify = QualifyAlterFunctionSchemaStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_FUNCTION,
	.address = AlterFunctionSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Routine_AlterOwner = {
	.deparse = DeparseAlterFunctionOwnerStmt,
	.qualify = QualifyAlterFunctionOwnerStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_FUNCTION,
	.address = AlterFunctionOwnerObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Routine_Drop = {
	.deparse = DeparseDropFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Routine_Grant = {
	.deparse = DeparseGrantOnFunctionStmt,
	.qualify = NULL,
	.preprocess = PreprocessGrantOnFunctionStmt,
	.postprocess = PostprocessGrantOnFunctionStmt,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Routine_Rename = {
	.deparse = DeparseRenameFunctionStmt,
	.qualify = QualifyRenameFunctionStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_FUNCTION,
	.address = RenameFunctionStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Schema_Drop = {
	.deparse = DeparseDropSchemaStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropSchemaStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Schema_Grant = {
	.deparse = DeparseGrantOnSchemaStmt,
	.qualify = NULL,
	.preprocess = PreprocessGrantOnSchemaStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Schema_Rename = {
	.deparse = DeparseAlterSchemaRenameStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_SCHEMA,
	.address = AlterSchemaRenameStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Statistics_Alter = {
	.deparse = DeparseAlterStatisticsStmt,
	.qualify = QualifyAlterStatisticsStmt,
	.preprocess = PreprocessAlterStatisticsStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Statistics_AlterObjectSchema = {
	.deparse = DeparseAlterStatisticsSchemaStmt,
	.qualify = QualifyAlterStatisticsSchemaStmt,
	.preprocess = PreprocessAlterStatisticsSchemaStmt,
	.postprocess = PostprocessAlterStatisticsSchemaStmt,
	.address = AlterStatisticsSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Statistics_AlterOwner = {
	.deparse = DeparseAlterStatisticsOwnerStmt,
	.qualify = QualifyAlterStatisticsOwnerStmt,
	.preprocess = PreprocessAlterStatisticsOwnerStmt,
	.postprocess = PostprocessAlterStatisticsOwnerStmt,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Statistics_Drop = {
	.deparse = NULL,
	.qualify = QualifyDropStatisticsStmt,
	.preprocess = PreprocessDropStatisticsStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Statistics_Rename = {
	.deparse = DeparseAlterStatisticsRenameStmt,
	.qualify = QualifyAlterStatisticsRenameStmt,
	.preprocess = PreprocessAlterStatisticsRenameStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Table_AlterTable = {
	.deparse = DeparseAlterTableStmt,
	.qualify = NULL,
	.preprocess = PreprocessAlterTableStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Table_AlterObjectSchema = {
	.deparse = DeparseAlterTableSchemaStmt,
	.qualify = QualifyAlterTableSchemaStmt,
	.preprocess = PreprocessAlterTableSchemaStmt,
	.postprocess = PostprocessAlterTableSchemaStmt,
	.address = AlterTableSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Table_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropTableStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Type_AlterObjectSchema = {
	.deparse = DeparseAlterTypeSchemaStmt,
	.qualify = QualifyAlterTypeSchemaStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_TYPE,
	.address = AlterTypeSchemaStmtObjectAddress,
	.markDistributed = false,
};

/*
 * PreprocessAlterViewSchemaStmt and PostprocessAlterViewSchemaStmt functions can be called
 * internally by ALTER TABLE view_name SET SCHEMA ... if the ALTER TABLE command targets a
 * view. In other words ALTER VIEW view_name SET SCHEMA will use the View_AlterObjectSchema
 * but ALTER TABLE view_name SET SCHEMA will use Table_AlterObjectSchema but call process
 * functions of View_AlterObjectSchema internally.
 */
static DistributeObjectOps View_AlterObjectSchema = {
	.deparse = DeparseAlterViewSchemaStmt,
	.qualify = QualifyAlterViewSchemaStmt,
	.preprocess = PreprocessAlterViewSchemaStmt,
	.postprocess = PostprocessAlterViewSchemaStmt,
	.address = AlterViewSchemaStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Type_AlterOwner = {
	.deparse = DeparseAlterTypeOwnerStmt,
	.qualify = QualifyAlterTypeOwnerStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = PostprocessAlterDistributedObjectStmt,
	.objectType = OBJECT_TYPE,
	.address = AlterTypeOwnerObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Type_AlterTable = {
	.deparse = DeparseAlterTypeStmt,
	.qualify = QualifyAlterTypeStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_TYPE,
	.address = AlterTypeStmtObjectAddress,
	.markDistributed = false,
};

/*
 * PreprocessAlterViewStmt and PostprocessAlterViewStmt functions can be called internally
 * by ALTER TABLE view_name SET/RESET ... if the ALTER TABLE command targets a view. In
 * other words ALTER VIEW view_name SET/RESET will use the View_AlterView
 * but ALTER TABLE view_name SET/RESET will use Table_AlterTable but call process
 * functions of View_AlterView internally.
 */
static DistributeObjectOps View_AlterView = {
	.deparse = DeparseAlterViewStmt,
	.qualify = QualifyAlterViewStmt,
	.preprocess = PreprocessAlterViewStmt,
	.postprocess = PostprocessAlterViewStmt,
	.address = AlterViewStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Type_Drop = {
	.deparse = DeparseDropTypeStmt,
	.qualify = NULL,
	.preprocess = PreprocessDropDistributedObjectStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Trigger_Drop = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = PreprocessDropTriggerStmt,
	.postprocess = NULL,
	.address = NULL,
	.markDistributed = false,
};
static DistributeObjectOps Type_Rename = {
	.deparse = DeparseRenameTypeStmt,
	.qualify = QualifyRenameTypeStmt,
	.preprocess = PreprocessAlterDistributedObjectStmt,
	.postprocess = NULL,
	.objectType = OBJECT_TYPE,
	.address = RenameTypeStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Vacuum_Analyze = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessVacuumStmt,
	.address = NULL,
	.markDistributed = false,
};

/*
 * PreprocessRenameViewStmt function can be called internally by ALTER TABLE view_name
 * RENAME ... if the ALTER TABLE command targets a view or a view's column. In other words
 * ALTER VIEW view_name RENAME will use the View_Rename but ALTER TABLE view_name RENAME
 * will use Any_Rename but call process functions of View_Rename internally.
 */
static DistributeObjectOps View_Rename = {
	.deparse = DeparseRenameViewStmt,
	.qualify = QualifyRenameViewStmt,
	.preprocess = PreprocessRenameViewStmt,
	.postprocess = NULL,
	.address = RenameViewStmtObjectAddress,
	.markDistributed = false,
};
static DistributeObjectOps Trigger_Rename = {
	.deparse = NULL,
	.qualify = NULL,
	.preprocess = NULL,
	.postprocess = PostprocessAlterTriggerRenameStmt,
	.address = NULL,
	.markDistributed = false,
};


/*
 * GetDistributeObjectOps looks up the DistributeObjectOps which handles the node.
 *
 * Never returns NULL.
 */
const DistributeObjectOps *
GetDistributeObjectOps(Node *node)
{
	switch (nodeTag(node))
	{
		case T_AlterDomainStmt:
		{
			return &Domain_Alter;
		}

		case T_AlterEnumStmt:
		{
			return &Any_AlterEnum;
		}

		case T_AlterExtensionStmt:
		{
			return &Any_AlterExtension;
		}

		case T_AlterExtensionContentsStmt:
		{
			return &Any_AlterExtensionContents;
		}

		case T_AlterFunctionStmt:
		{
			return &Any_AlterFunction;
		}

		case T_AlterForeignServerStmt:
		{
			return &Any_AlterForeignServer;
		}

		case T_AlterObjectDependsStmt:
		{
			AlterObjectDependsStmt *stmt = castNode(AlterObjectDependsStmt, node);
			switch (stmt->objectType)
			{
				case OBJECT_FUNCTION:
				{
					return &Function_AlterObjectDepends;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_AlterObjectDepends;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_AlterObjectDepends;
				}

				case OBJECT_TRIGGER:
				{
					return &Trigger_AlterObjectDepends;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_AlterObjectSchemaStmt:
		{
			AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
			switch (stmt->objectType)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_AlterObjectSchema;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_AlterObjectSchema;
				}

				case OBJECT_DOMAIN:
				{
					return &Domain_AlterObjectSchema;
				}

				case OBJECT_EXTENSION:
				{
					return &Extension_AlterObjectSchema;
				}

				case OBJECT_FUNCTION:
				{
					return &Function_AlterObjectSchema;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_AlterObjectSchema;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_AlterObjectSchema;
				}

				case OBJECT_SEQUENCE:
				{
					return &Sequence_AlterObjectSchema;
				}

				case OBJECT_STATISTIC_EXT:
				{
					return &Statistics_AlterObjectSchema;
				}

				case OBJECT_FOREIGN_TABLE:
				case OBJECT_TABLE:
				{
					return &Table_AlterObjectSchema;
				}

				case OBJECT_TSCONFIGURATION:
				{
					return &TextSearchConfig_AlterObjectSchema;
				}

				case OBJECT_TSDICTIONARY:
				{
					return &TextSearchDict_AlterObjectSchema;
				}

				case OBJECT_TYPE:
				{
					return &Type_AlterObjectSchema;
				}

				case OBJECT_VIEW:
				{
					return &View_AlterObjectSchema;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_AlterOwnerStmt:
		{
			AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
			switch (stmt->objectType)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_AlterOwner;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_AlterOwner;
				}

				case OBJECT_DATABASE:
				{
					return &Database_AlterOwner;
				}

				case OBJECT_DOMAIN:
				{
					return &Domain_AlterOwner;
				}

				case OBJECT_FOREIGN_SERVER:
				{
					return &ForeignServer_AlterOwner;
				}

				case OBJECT_FUNCTION:
				{
					return &Function_AlterOwner;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_AlterOwner;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_AlterOwner;
				}

				case OBJECT_STATISTIC_EXT:
				{
					return &Statistics_AlterOwner;
				}

				case OBJECT_TSCONFIGURATION:
				{
					return &TextSearchConfig_AlterOwner;
				}

				case OBJECT_TSDICTIONARY:
				{
					return &TextSearchDict_AlterOwner;
				}

				case OBJECT_TYPE:
				{
					return &Type_AlterOwner;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_AlterPolicyStmt:
		{
			return &Any_AlterPolicy;
		}

		case T_AlterRoleStmt:
		{
			return &Any_AlterRole;
		}

		case T_AlterRoleSetStmt:
		{
			return &Any_AlterRoleSet;
		}

		case T_AlterSeqStmt:
		{
			return &Sequence_Alter;
		}

		case T_AlterStatsStmt:
		{
			return &Statistics_Alter;
		}

		case T_AlterTableStmt:
		{
			AlterTableStmt *stmt = castNode(AlterTableStmt, node);
			switch (AlterTableStmtObjType_compat(stmt))
			{
				case OBJECT_TYPE:
				{
					return &Type_AlterTable;
				}

				case OBJECT_TABLE:
				{
					return &Table_AlterTable;
				}

				case OBJECT_FOREIGN_TABLE:
				{
					return &ForeignTable_AlterTable;
				}

				case OBJECT_INDEX:
				{
					return &Index_AlterTable;
				}

				case OBJECT_SEQUENCE:
				{
					return &Sequence_AlterOwner;
				}

				case OBJECT_VIEW:
				{
					return &View_AlterView;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_AlterTableMoveAllStmt:
		{
			return &Any_AlterTableMoveAll;
		}

		case T_AlterTSConfigurationStmt:
		{
			return &TextSearchConfig_Alter;
		}

		case T_AlterTSDictionaryStmt:
		{
			return &TextSearchDict_Alter;
		}

		case T_ClusterStmt:
		{
			return &Any_Cluster;
		}

		case T_CommentStmt:
		{
			CommentStmt *stmt = castNode(CommentStmt, node);
			switch (stmt->objtype)
			{
				case OBJECT_TSCONFIGURATION:
				{
					return &TextSearchConfig_Comment;
				}

				case OBJECT_TSDICTIONARY:
				{
					return &TextSearchDict_Comment;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_CompositeTypeStmt:
		{
			return &Any_CompositeType;
		}

		case T_CreateDomainStmt:
		{
			return &Any_CreateDomain;
		}

		case T_CreateEnumStmt:
		{
			return &Any_CreateEnum;
		}

		case T_CreateExtensionStmt:
		{
			return &Any_CreateExtension;
		}

		case T_CreateFunctionStmt:
		{
			return &Any_CreateFunction;
		}

		case T_CreateForeignServerStmt:
		{
			return &Any_CreateForeignServer;
		}

		case T_CreatePolicyStmt:
		{
			return &Any_CreatePolicy;
		}

		case T_CreateRoleStmt:
		{
			return &Any_CreateRole;
		}

		case T_CreateSchemaStmt:
		{
			return &Any_CreateSchema;
		}

		case T_CreateStatsStmt:
		{
			return &Any_CreateStatistics;
		}

		case T_CreateTrigStmt:
		{
			return &Any_CreateTrigger;
		}

		case T_DefineStmt:
		{
			DefineStmt *stmt = castNode(DefineStmt, node);
			switch (stmt->kind)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_Define;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_Define;
				}

				case OBJECT_TSCONFIGURATION:
				{
					return &TextSearchConfig_Define;
				}

				case OBJECT_TSDICTIONARY:
				{
					return &TextSearchDict_Define;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_DropRoleStmt:
		{
			return &Any_DropRole;
		}

		case T_DropStmt:
		{
			DropStmt *stmt = castNode(DropStmt, node);
			switch (stmt->removeType)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_Drop;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_Drop;
				}

				case OBJECT_DOMAIN:
				{
					return &Domain_Drop;
				}

				case OBJECT_EXTENSION:
				{
					return &Extension_Drop;
				}

				case OBJECT_FUNCTION:
				{
					return &Function_Drop;
				}

				case OBJECT_FOREIGN_SERVER:
				{
					return &ForeignServer_Drop;
				}

				case OBJECT_INDEX:
				{
					return &Index_Drop;
				}

				case OBJECT_POLICY:
				{
					return &Policy_Drop;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_Drop;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_Drop;
				}

				case OBJECT_SCHEMA:
				{
					return &Schema_Drop;
				}

				case OBJECT_SEQUENCE:
				{
					return &Sequence_Drop;
				}

				case OBJECT_STATISTIC_EXT:
				{
					return &Statistics_Drop;
				}

				case OBJECT_TABLE:
				{
					return &Table_Drop;
				}

				case OBJECT_TSCONFIGURATION:
				{
					return &TextSearchConfig_Drop;
				}

				case OBJECT_TSDICTIONARY:
				{
					return &TextSearchDict_Drop;
				}

				case OBJECT_TYPE:
				{
					return &Type_Drop;
				}

				case OBJECT_TRIGGER:
				{
					return &Trigger_Drop;
				}

				case OBJECT_VIEW:
				{
					return &View_Drop;
				}

				default:
				{
					return &NoDistributeOps;
				}
			}
		}

		case T_GrantRoleStmt:
		{
			return &Any_GrantRole;
		}

		case T_GrantStmt:
		{
			GrantStmt *stmt = castNode(GrantStmt, node);
			switch (stmt->objtype)
			{
				case OBJECT_SCHEMA:
				{
					return &Schema_Grant;
				}

				case OBJECT_SEQUENCE:
				{
					return &Sequence_Grant;
				}

				case OBJECT_FDW:
				{
					return &FDW_Grant;
				}

				case OBJECT_FOREIGN_SERVER:
				{
					return &ForeignServer_Grant;
				}

				case OBJECT_FUNCTION:
				{
					return &Function_Grant;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_Grant;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_Grant;
				}

				default:
				{
					return &Any_Grant;
				}
			}
		}

		case T_IndexStmt:
		{
			return &Any_Index;
		}

		case T_ViewStmt:
		{
			return &Any_View;
		}

		case T_ReindexStmt:
		{
			return &Any_Reindex;
		}

		case T_VacuumStmt:
		{
			return &Vacuum_Analyze;
		}

		case T_RenameStmt:
		{
			RenameStmt *stmt = castNode(RenameStmt, node);
			switch (stmt->renameType)
			{
				case OBJECT_AGGREGATE:
				{
					return &Aggregate_Rename;
				}

				case OBJECT_ATTRIBUTE:
				{
					return &Attribute_Rename;
				}

				case OBJECT_COLLATION:
				{
					return &Collation_Rename;
				}

				case OBJECT_DOMAIN:
				{
					return &Domain_Rename;
				}

				case OBJECT_DOMCONSTRAINT:
				{
					return &Domain_RenameConstraint;
				}

				case OBJECT_FOREIGN_SERVER:
				{
					return &ForeignServer_Rename;
				}

				case OBJECT_FUNCTION:
				{
					return &Function_Rename;
				}

				case OBJECT_PROCEDURE:
				{
					return &Procedure_Rename;
				}

				case OBJECT_ROUTINE:
				{
					return &Routine_Rename;
				}

				case OBJECT_SCHEMA:
				{
					return &Schema_Rename;
				}

				case OBJECT_SEQUENCE:
				{
					return &Sequence_Rename;
				}

				case OBJECT_STATISTIC_EXT:
				{
					return &Statistics_Rename;
				}

				case OBJECT_TSCONFIGURATION:
				{
					return &TextSearchConfig_Rename;
				}

				case OBJECT_TSDICTIONARY:
				{
					return &TextSearchDict_Rename;
				}

				case OBJECT_TYPE:
				{
					return &Type_Rename;
				}

				case OBJECT_TRIGGER:
				{
					return &Trigger_Rename;
				}

				case OBJECT_VIEW:
				{
					return &View_Rename;
				}

				case OBJECT_COLUMN:
				{
					switch (stmt->relationType)
					{
						case OBJECT_VIEW:
						{
							return &View_Rename;
						}

						default:
						{
							return &Any_Rename;
						}
					}
				}

				default:
				{
					return &Any_Rename;
				}
			}
		}

		default:
		{
			return &NoDistributeOps;
		}
	}
}
