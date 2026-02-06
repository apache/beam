---
type: languages
title: "Beam SQL DDL: Alter"
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

# ALTER statements

The **ALTER** statement modifies the definition of an existing Catalog or Table.
For supported tables (like Iceberg), this enables **schema and partition evolution**.

## ALTER CATALOG
Modifies an existing catalog's properties.

```sql
ALTER CATALOG catalog_name
    [ SET ( 'key' = 'val', ... ) ]
    [ RESET ( 'key', ... ) ]
```
- **SET**: Adds new properties or updates existing ones.
- **RESET** / **UNSET**: Removes properties.

## ALTER TABLE
Modifies an existing table's properties and evolves its partition and schema.

```sql
ALTER TABLE table_name
    [ ADD COLUMNS ( col_def, ... ) ]
    [ DROP COLUMNS ( col_name, ... ) ]
    [ ADD PARTITIONS ( partition_field, ... ) ]
    [ DROP PARTITIONS ( partition_field, ... ) ]
    [ SET ( 'key' = 'val', ... ) ]
    [ ( RESET | UNSET ) ( 'key', ... ) ];
```

*Example 1: Add or remove columns*
```sql
-- Add columns
ALTER TABLE orders ADD COLUMNS (
    customer_email VARCHAR,
    shipping_region VARCHAR
);

-- Drop columns
ALTER TABLE orders DROP COLUMNS ( customer_email );
```

*Example 2: Modify partition spec*
```sql
-- Add a partition field
ALTER TABLE orders ADD PARTITIONS ( 'year(order_date)' );

-- Remove a partition field
ALTER TABLE orders DROP PARTITIONS ( 'region_id' );
```

*Example 3: Modify table properties*
```sql
ALTER TABLE orders SET (
    'write.format.default' = 'orc',
    'write.metadata.metrics.default' = 'full' );
ALTER TABLE orders RESET ( 'write.target-file-size-bytes' );
```
