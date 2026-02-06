---
type: languages
title: "Beam SQL DDL: Show"
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

# SHOW statements

The **SHOW** statement are used to list objects within a parent container
(e.g. databases within a catalog), or inspect the currently active context.
Results can be filtered using regex patterns.

## SHOW CATALOGS
Lists all registered catalogs (name and type). Supports regex filtering.

_**Example**: List all catalogs_
```sql
SHOW CATALOGS;
```

_**Example**: List catalogs matching a pattern_
```sql
SHOW CATALOGS LIKE 'prod_%';
```

_**Example**: Verify which catalog is currently active_
```sql
SHOW CURRENT CATALOG;
```

## SHOW DATABASES
Lists databases under the currently active catalog, or a specified catalog.

_**Example**: List databases in the currently active catalog_
```sql
SHOW DATABASES;
```

_**Example**: List databases in a specified catalog_
```sql
SHOW DATABASES IN my_catalog;
```

_**Example**: List databases matching a pattern_
```sql
SHOW DATABASES IN my_catalog LIKE '%geo%';
```

_**Example**: Verify which database is currently active_
```sql
SHOW CURRENT DATABASE;
```

## SHOW TABLES
Lists tables under the currently active database, or a specified database.

_**Example**: List tables in the currently active database_
```sql
SHOW TABLES;
```

_**Example**: List databases in a specified database_
```sql
SHOW TABLES IN my_db;
SHOW TABLES IN my_catalog.my_db;
```

_**Example**: List databases matching a pattern_
```sql
SHOW TABLES IN my_db LIKE '%orders%';
```
