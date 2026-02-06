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

Beam SQL provides a robust hierarchy for managing metadata for external data sources. Instead of treating all tables as flat objects, Beam SQL utilizes a three-tier namespace system:
1. Catalog: The highest level container (e.g. a Glue Catalog connected to S3 or a BigLake Catalog connected to GCS).
2. Database: A logical namespace within a Catalog (often maps to "namespace" in systems like Iceberg).
3. Table: The actual data entity containing schema and rows.

This structure allows users to connect to multiple disparate systems (e.g., a production BigQuery catalog and a dev Iceberg catalog) simultaneously and switch contexts seamlessly.
It also allows interactions between these systems via cross-catalog queries.

Click below to learn about metadata management at each level:

## Catalogs
The Catalog is the entry point for external metadata. When you initialize Beam SQL, you start off with a `default` catalog that contains a `default` database.
You can register new catalogs, switch between them, and modify their configurations.

### [CREATE](/create#CREATE-CATALOG)
### [USE](/use#USE-CATALOG)
### [DROP](/drop#DROP-CATALOG)
### [ALTER](/alter#ALTER-CATALOG)
### [SHOW](/show#SHOW-CATALOGS)

## Databases

### [CREATE](/create#CREATE-DATABASE)
### [USE](/use#USE-DATABASE)
### [DROP](/drop#DROP-DATABASE)
### [SHOW](/show#SHOW-DATABASES)

## Tables

### [CREATE](/create#CREATE-TABLE)
### [DROP](/drop#DROP-TABLE)
### [ALTER](/alter#ALTER-TABLE)
### [SHOW](/show#SHOW-TABLES)
