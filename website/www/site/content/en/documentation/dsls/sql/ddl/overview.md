---
type: languages
title: "Beam SQL DDL Overview"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Beam SQL DDL Overview

Beam SQL provides a standard three-level hierarchy to manage metadata across external data sources,
enabling structured discovery and cross-source interoperability.
1. Catalog: The top-level container representing an external metadata provider. Examples include a Hive Metastore, AWS Glue, or a BigLake Catalog.
2. Database: A logical grouping within a Catalog. This typically maps to a "Schema" in traditional RDBMS or a "Namespace" in systems like Apache Iceberg
3. Table: The leaf node containing the schema definition and the underlying data.

This structure enables Federated Querying. Because Beam can resolve multiple catalogs simultaneously,
you can execute complex pipelines that bridge disparate environments within a single SQL statement (e.g.
joining a production BigQuery table with a developmental Iceberg dataset in GCS).

By using fully qualified names (e.g., catalog.database.table), you can perform cross-catalog joins or
migrate data between clouds without manual schema mapping or intermediate storage.

Click below to learn about metadata management at each level:

## Catalogs
The Catalog is the entry point for external metadata. When you initialize Beam SQL, you start off with a `default` catalog that contains a `default` database.
You can register new catalogs, switch between them, and modify their configurations.

### [CREATE](/documentation/dsls/sql/ddl/create#CREATE-CATALOG)
### [USE](/documentation/dsls/sql/ddl/use#USE-CATALOG)
### [DROP](/documentation/dsls/sql/ddl/drop#DROP-CATALOG)
### [ALTER](/documentation/dsls/sql/ddl/alter#ALTER-CATALOG)
### [SHOW](/documentation/dsls/sql/ddl/show#SHOW-CATALOGS)

## Databases

### [CREATE](/documentation/dsls/sql/ddl/create#CREATE-DATABASE)
### [USE](/documentation/dsls/sql/ddl/use#USE-DATABASE)
### [DROP](/documentation/dsls/sql/ddl/drop#DROP-DATABASE)
### [SHOW](/documentation/dsls/sql/ddl/show#SHOW-DATABASES)

## Tables

### [CREATE](/documentation/dsls/sql/ddl/create#CREATE-TABLE)
### [DROP](/documentation/dsls/sql/ddl/drop#DROP-TABLE)
### [ALTER](/documentation/dsls/sql/ddl/alter#ALTER-TABLE)
### [SHOW](/documentation/dsls/sql/ddl/show#SHOW-TABLES)
