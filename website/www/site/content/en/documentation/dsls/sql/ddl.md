---
type: languages
title: "Beam SQL DDL"
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

# Beam SQL DDL

Beam SQL provides a standard three-level hierarchy to manage metadata across external data sources,
enabling structured discovery and cross-source interoperability.
1. Catalog: The top-level container representing an external metadata provider. Examples include a Hive Metastore, AWS Glue, or a BigLake Catalog.
2. Database: A logical grouping within a Catalog. This typically maps to a "Schema" in traditional RDBMS or a "Namespace" in systems like Apache Iceberg
3. Table: The leaf node containing the schema definition and the underlying data.

This structure enables Federated Querying. Because Beam can resolve multiple Catalogs simultaneously,
you can execute complex pipelines that bridge disparate environments within a single SQL statement (e.g.
joining a production BigQuery table with a developmental Iceberg dataset in GCS).

By using fully qualified names (e.g., catalog.database.table), you can perform cross-catalog joins or
migrate data between clouds without manual schema mapping or intermediate storage.

Below are details about metadata management at each level:

## Catalogs
The Catalog is the entry point for external metadata. When you initialize Beam SQL, you start off with a `default` Catalog that contains a `default` Database.
You can register new Catalogs, switch between them, and modify their configurations.

{{< tab CREATE >}}
<p>Registers a new Catalog instance</p>
<p><i><strong>Note</strong>: Creating a Catalog does not automatically switch to it. Remember
to run <code>USE CATALOG</code> afterwards to set it.</i></p>

{{< highlight >}}
CREATE CATALOG [ IF NOT EXISTS ] catalog_name
TYPE 'type_name'
[ PROPERTIES ( 'key' = 'value' [, ...] ) ]
{{< /highlight >}}

<p><i><strong>Example</strong>: Creating a Hadoop Catalog (Local Storage)</i></p>
{{< highlight >}}
CREATE CATALOG local_catalog
TYPE iceberg
PROPERTIES (
  'type'      = 'hadoop',
  'warehouse' = 'file:///tmp/iceberg-warehouse'
)
{{< /highlight >}}

<p><i><strong>Example</strong>: Registering a BigLake Catalog (GCS)</i></p>
{{< highlight >}}
CREATE CATALOG prod_iceberg
TYPE iceberg
PROPERTIES (
  'type'           = 'rest',
  'uri'            = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
  'warehouse'      = 'gs://my-company-bucket/warehouse',
  'header.x-goog-user-project' = 'my_prod_project',
  'rest.auth.type' = 'google',
  'io-impl'        = 'org.apache.iceberg.gcp.gcs.GCSFileIO',
  'header.X-Iceberg-Access-Delegation' = 'vended-credentials'
);
{{< /highlight >}}
{{< /tab >}}
{{< tab USE >}}
<p>Sets the active Catalog for the current session. This simplifies queries by allowing you
to reference Databases directly without their fully-qualified names (e.g. <code>my_db</code> instead of <code>my_catalog.my_db</code>)</p>

<p><i><strong>Tip:</strong> run <code>SHOW CURRENT CATALOG</code> to view the currently active Catalog.</i></p>
<p><i><strong>Note:</strong> All subsequent DATABASE and TABLE commands will be executed under this Catalog, unless fully qualified.</i></p>

{{< highlight >}}
USE CATALOG prod_iceberg;
{{< /highlight >}}
{{< /tab >}}
{{< tab ALTER >}}
Modifies the properties of a registered Catalog.
{{< highlight >}}
ALTER CATALOG catalog_name
[ SET ( 'key' = 'val', ... ) ]
[ RESET ( 'key', ... ) ]
{{< /highlight >}}
<ol>
  <li><strong>SET:</strong> Adds new properties or updates existing ones.</li>
  <li><strong>RESET / UNSET:</strong> Removes properties.</li>
</ol>
<br>
{{< /tab >}}
{{< tab SHOW >}}
<p>Can be used to either:</p>
<ol>
  <li>List Catalogs registered in this Beam SQL session.</li>
  <li>View the currently active Catalog.</li>
</ol>

{{< highlight >}}
SHOW CATALOGS [ LIKE regex_pattern ]
{{< /highlight >}}

<p><i><strong>Example</strong>: List all Catalogs</i></p>
{{< highlight >}}
SHOW CATALOGS;
{{< /highlight >}}

<p><i><strong>Example</strong>: List Catalogs matching a pattern</i></p>
{{< highlight >}}
SHOW CATALOGS LIKE 'prod_%';
{{< /highlight >}}

<p><i><strong>Example</strong>: Verify which Catalog is currently active</i></p>
{{< highlight >}}
SHOW CURRENT CATALOG;
{{< /highlight >}}
{{< /tab >}}
{{< tab DROP >}}
<p>Unregisters a Catalog from the current Beam SQL session. This does not destroy external data.</p>

{{< highlight >}}
DROP CATALOG [ IF EXISTS ] catalog_name;
{{< /highlight >}}
{{< /tab >}}

## Databases
A Database lives inside a Catalog and may contain a number of Tables.

{{< tab CREATE >}}
<p>Creates a new Database within the current Catalog (default), or the specified Catalog.</p>
<p><i><strong>Note</strong>: Creating a Database does not automatically switch to it. Remember
to run <code>USE DATABASE</code> afterwards to set it.</i></p>

{{< highlight >}}
CREATE DATABASE [ IF NOT EXISTS ] [ catalog_name. ]database_name;
{{< /highlight >}}

<p><i><strong>Example</strong>: Create a Database in the current active Catalog</i></p>
{{< highlight >}}
USE CATALOG my_catalog;
CREATE DATABASE sales_data;
{{< /highlight >}}

<p><i><strong>Example</strong>: Create a Database in a specified registered Catalog</i></p>
{{< highlight >}}
CREATE DATABASE other_catalog.sales_data;
{{< /highlight >}}
{{< /tab >}}
{{< tab USE >}}
<p>Sets the active Database for the current session. This simplifies queries by allowing you
to reference Databases directly without their fully-qualified names (e.g. <code>my_db</code> instead of <code>my_catalog.my_db</code>)</p>

<p><i><strong>Note:</strong> All subsequent TABLE commands will be executed under this Database, unless fully qualified.</i></p>

{{< highlight >}}
USE DATABASE sales_data;
{{< /highlight >}}

<p><i>Switch to a Database in a specified Catalog. <strong>Node</strong>: this also switches the default to that Catalog</i></p>
{{< highlight >}}
USE DATABASE other_catalog.sales_data;
{{< /highlight >}}
{{< /tab >}}
{{< tab SHOW >}}
<p>Can be used to either:</p>
<ol>
  <li>List Databases within the currently active Catalog, or a specified Catalog.</li>
  <li>View the currently active Database.</li>
</ol>

{{< highlight >}}
SHOW DATABASES [ ( FROM | IN )? catalog_name ] [LIKE regex_pattern ]
{{< /highlight >}}

<p><i><strong>Example</strong>: List Databases in the currently active Catalog</i></p>
{{< highlight >}}
SHOW DATABASES;
{{< /highlight >}}

<p><i><strong>Example</strong>: List Databases in a specified Catalog</i></p>
{{< highlight >}}
SHOW DATABASES IN my_catalog;
{{< /highlight >}}

<p><i><strong>Example</strong>: List Databases matching a pattern</i></p>
{{< highlight >}}
SHOW DATABASES IN my_catalog LIKE '%geo%';
{{< /highlight >}}

<p><i><strong>Example</strong>: Verify which Database is currently active</i></p>
{{< highlight >}}
SHOW CURRENT DATABASE;
{{< /highlight >}}
{{< /tab >}}
{{< tab DROP >}}
<p>Unregisters a Database from the current session. For some connectors, this
will also <strong>delete</strong> the Database from the external data source.</p>

{{< highlight >}}
DROP DATABASE [ IF EXISTS ] database_name [ RESTRICT | CASCADE ];
{{< /highlight >}}

<ol>
  <li><strong>RESTRICT:</strong> (Default): Fails if the Database is not empty.</li>
  <li><strong>CASCADE:</strong> Drops the Database and all tables contained within it. <strong>Use with caution</strong>.</li>
</ol>
<br>
{{< /tab >}}

## Tables
The actual entity containing data, and is described by a schema. Some
data sources also support applying a partition spec and attaching table-specific properties.

{{< tab CREATE >}}
<p>Creates a new Table within the current Catalog and Database (default), or the specified Catalog and Database.</p>

{{< highlight >}}
CREATE EXTERNAL TABLE [ IF NOT EXISTS ] [ catalog. ][ db. ]table_name (
  col_name col_type [ NOT NULL ] [ COMMENT 'col_comment' ],
    ...
  )
  TYPE 'type_name'
  [ PARTITIONED BY ( 'partition_field' [, ... ] ) ]
  [ COMMENT 'table_comment' ]
  [ LOCATION 'location_uri' ]
  [ TBLPROPERTIES 'properties_json_string' ];
{{< /highlight >}}
<ul>
  <li><strong>TYPE:</strong> the table type (e.g. <code>'iceberg'</code>, <code>'text'</code>, <code>'kafka'</code>).</li>
  <li><strong>PARTITIONED BY:</strong> an ordered list of fields describing the partition spec.</li>
  <li><strong>LOCATION:</strong> explicitly sets the location of the table (overriding the inferred <code>catalog.db.table_name</code> location)</li>
  <li><strong>TBLPROPERTIES:</strong> configuration properties used when creating the table or setting up its IO connection.</li>
</ul>
<br>

<p><i><strong>Example</strong>: Creating an Iceberg Table</i></p>
{{< highlight >}}
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
{{< /highlight >}}

<ul>
  <li>This creates an Iceberg table named <code>orders</code> under the namespace <code>sales_data</code>, within the <code>prod_iceberg</code> catalog.</li>
  <li>The table is partitioned by <code>region_id</code>, then by the day value of <code>order_date</code> (using Iceberg's <a href="https://iceberg.apache.org/docs/latest/partitioning/#icebergs-hidden-partitioning">hidden partitioning</a>).</li>
  <li>The table is created with the appropriate properties <code>"write.format.default"</code> and <code>"read.split.target-size"</code>. The Beam property <code>"beam.write.triggering_frequency_seconds"</code></li>
  <li>Beam properties (prefixed with <code>"beam.write."</code> and <code>"beam.read."</code> are intended for the relevant IOs)</li>
</ul>
{{< /tab >}}
{{< tab ALTER >}}
Modifies an existing Table's properties and evolves its partition and schema.
{{< highlight >}}
ALTER TABLE table_name
    [ ADD COLUMNS ( col_def, ... ) ]
    [ DROP COLUMNS ( col_name, ... ) ]
    [ ADD PARTITIONS ( partition_field, ... ) ]
    [ DROP PARTITIONS ( partition_field, ... ) ]
    [ SET ( 'key' = 'val', ... ) ]
    [ ( RESET | UNSET ) ( 'key', ... ) ];
{{< /highlight >}}

<p><i><strong>Example</strong>: Add or remove columns</i></p>
{{< highlight >}}
-- Add columns
ALTER TABLE orders ADD COLUMNS (
    customer_email VARCHAR,
    shipping_region VARCHAR
);

-- Drop columns
ALTER TABLE orders DROP COLUMNS ( customer_email );
{{< /highlight >}}

<p><i><strong>Example</strong>: Modify partition spec</i></p>
{{< highlight >}}
-- Add a partition field
ALTER TABLE orders ADD PARTITIONS ( 'year(order_date)' );

-- Remove a partition field
ALTER TABLE orders DROP PARTITIONS ( 'region_id' );
{{< /highlight >}}

<p><i><strong>Example</strong>: Modify table properties</i></p>
{{< highlight >}}
ALTER TABLE orders SET (
    'write.format.default' = 'orc',
    'write.metadata.metrics.default' = 'full' );

ALTER TABLE orders RESET ( 'write.target-file-size-bytes' );
{{< /highlight >}}

{{< /tab >}}
{{< tab SHOW >}}
<p>Lists tables under the currently active database, or a specified database.</p>

{{< highlight >}}
SHOW TABLES [ ( FROM | IN )? [ catalog_name '.' ] database_name ] [ LIKE regex_pattern ]
{{< /highlight >}}

<p><i><strong>Example</strong>: List tables in the currently active database and catalog</i></p>
{{< highlight >}}
SHOW TABLES;
{{< /highlight >}}

<p><i><strong>Example</strong>: List tables in a specified database</i></p>
{{< highlight >}}
SHOW TABLES IN my_db;
SHOW TABLES IN my_catalog.my_db;
{{< /highlight >}}

<p><i><strong>Example</strong>: List tables matching a pattern</i></p>
{{< highlight >}}
SHOW TABLES IN my_db LIKE '%orders%';
{{< /highlight >}}

{{< /tab >}}
{{< tab DROP >}}
<p>Unregisters a table from the current session. For supported connectors, this
will also <strong>delete</strong> the table from the external data source.</p>

{{< highlight >}}
DROP TABLE [ IF EXISTS ] table_name;
{{< /highlight >}}
{{< /tab >}}
