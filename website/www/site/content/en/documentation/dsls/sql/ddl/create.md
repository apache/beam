---
type: languages
title: "Beam SQL DDL: Create"
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

# CREATE statements

The **CREATE** command serves two potential functions depending on the connector:

- **Registration**: By default, it registers an existing external entity in the Beam SQL session.
- **Instantiation**: For supported connectors (e.g., Iceberg), it physically creates the entity
(e.g. namespace or table) in the underlying storage.

_**Note**: Creating a catalog or database does not automatically switch to it. Remember
to run [USE](TODO:LINK-TO-USE) afterwards to set it as a default._

## `CREATE CATALOG`
Registers a new catalog instance.

```sql
CREATE CATALOG [ IF NOT EXISTS ] catalog_name
TYPE 'type_name'
[ PROPERTIES ( 'key' = 'value' [, ...] ) ]
```

_**Example**: Creating a Hadoop Catalog (Local Storage)_
```sql
CREATE CATALOG local_catalog
TYPE iceberg
PROPERTIES (
    'type'      = 'hadoop',
    'warehouse' = 'file:///tmp/iceberg-warehouse'
)
```

_**Example**: Registering a BigLake Catalog (GCS)_
```sql
CREATE CATALOG prod_iceberg
TYPE iceberg
PROPERTIES (
    'type'           = 'rest',
    'uri'            = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    'warehouse'      = 'gs://my-company-bucket/warehouse',
    'header.x-goog-user-project' = 'my_prod_project',
    'rest.auth.type' = 'org.apache.iceberg.gcp.auth.GoogleAuthManager',
    'io-impl'        = 'org.apache.iceberg.gcp.gcs.GCSFileIO',
    'rest-metrics-reporting-enabled' = 'false'
);
```

### `CREATE DATABASE`
Creates a new Database within the current Catalog (default), or the specified Catalog.
```sql
CREATE DATABASE [ IF NOT EXISTS ] [ catalog_name. ]database_name;
```

_**Example**: Create a database in the current active catalog_
```sql
USE CATALOG my_catalog;
CREATE DATABASE sales_data;
```

_**Example**: Create a database in a specified catalog (must be registered)_
```sql
CREATE DATABASE other_catalog.sales_data;
```

### `CREATE TABLE`
Creates a table within the currently active catalog and database. If the table name is fully qualified, the referenced database and catalog is used.

```sql
CREATE EXTERNAL TABLE [ IF NOT EXISTS ] [ catalog. ][ db. ]table_name (
    col_name col_type [ NOT NULL ] [ COMMENT 'col_comment' ],
    ...
)
TYPE 'type_name'
[ PARTITIONED BY ( 'partition_field' [, ... ] ) ]
[ COMMENT 'table_comment' ]
[ LOCATION 'location_uri' ]
[ TBLPROPERTIES 'properties_json_string' ];
```
- **TYPE**: the table type (e.g. `'iceberg'`, `'text'`, `'kafka'`)
- **PARTITIONED BY**: an ordered list of fields describing the partition spec.
- **LOCATION**: explicitly sets the location of the table (overriding the inferred `catalog.db.table_name` location)
- **TBLPROPERTIES**: configuration properties used when creating the table or setting up its IO connection.

_**Example**: Creating an Iceberg Table_
```sql
CREATE EXTERNAL TABLE prod_iceberg.sales_data.orders (
    order_id BIGINT NOT NULL COMMENT 'Unique order identifier',
    amount DECIMAL(10, 2),
    order_date TIMESTAMP,
    region_id VARCHAR
)
TYPE 'iceberg'
PARTITIONED BY ( 'region_id', 'day(order_date)' )
COMMENT 'Daily sales transactions'
TBLPROPERTIES '{
    "write.format.default": "parquet",
    "read.split.target-size": 268435456",
    "beam.write.triggering_frequency_seconds": 60"
}';
```
- This creates an Iceberg table named `orders` under the namespace `sales_data`, within the `prod_iceberg` catalog.
- The table is partitioned by `region_id`, then by the day value of `order_date` (using Iceberg's [hidden partitioning](https://iceberg.apache.org/docs/latest/partitioning/#icebergs-hidden-partitioning)).
- The table is created with the appropriate properties `"write.format.default"` and `"read.split.target-size"`. The Beam property `"beam.write.triggering_frequency_seconds"`
- Beam properties (prefixed with `"beam.write."` and `"beam.read."` are intended for the relevant IOs)
