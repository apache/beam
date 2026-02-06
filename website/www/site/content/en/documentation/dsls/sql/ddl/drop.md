---
type: languages
title: "Beam SQL DDL: Drop"
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

# DROP statements
The **DROP** command serves two potential functions depending on the connector:

- **Unregistration**: unregisters an entity from the current Beam SQL session.
- **Deletion**: For supported connectors (like **Iceberg**), it **physically deletes** the entity
  (e.g. namespace or table) in the underlying storage.

  **Caution:** Physical deletion can be permanent

## DROP CATALOG
Unregisters a catalog from Beam SQL. This does not destroy external data, only the link within the SQL session.

```sql
DROP CATALOG [ IF EXISTS ] catalog_name;
```

## DROP DATABASE
Unregisters a database from the current session. For supported connectors, this
will also **delete** the database from the external data source.

```sql
DROP DATABASE [ IF EXISTS ] database_name [ RESTRICT | CASCADE ];
```
- **RESTRICT** (Default): Fails if the database is not empty.
- **CASCADE**: Drops the database and all tables contained within it. **Use with caution.**

## DROP TABLE
Unregisters a table from the current session. For supported connectors, this
will also **delete** the table from the external data source.
```sql
DROP TABLE [ IF EXISTS ] table_name;
```
