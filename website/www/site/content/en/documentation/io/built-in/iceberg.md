---
title: "Apache Iceberg I/O connector"
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

[Built-in I/O Transforms](/documentation/io/built-in/)


# Apache Iceberg I/O connector

The Beam SDKs include built-in transforms that can read data from and write data
to [Apache Iceberg](https://iceberg.apache.org/) tables.

{{< language-switcher sql java py yaml>}}

{{< paragraph class="language-java" >}}
To use IcebergIO, add the Maven artifact dependency to your `pom.xml` file.
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-sdks-java-io-iceberg</artifactId>
  <version>{{< param release_latest >}}</version>
</dependency>
{{< /highlight >}}

{{< paragraph class="language-sql" >}}
To use IcebergIO, install the [Beam SQL Shell](https://beam.apache.org/documentation/dsls/sql/shell/#installation) and run the following command:
{{< /paragraph >}}

{{% section class="language-sql" %}}
```shell
./beam-sql.sh --io iceberg
```
{{% /section %}}

{{< paragraph class="language-yaml" >}}
To use IcebergIO with [Beam YAML](../../sdks/yaml), install the `yaml` extra:
{{< /paragraph >}}

{{< highlight yaml >}}
pip install apache_beam[yaml]
{{< /highlight >}}

{{< paragraph >}}
If you're new to Iceberg, check out the [basics section](#iceberg-basics) under the guide.
{{< /paragraph >}}

Additional resources:


{{< paragraph wrap="span" >}}
* [IcebergIO configuration parameters](https://beam.apache.org/documentation/io/managed-io/#iceberg-write)
* [IcebergIO source code](https://github.com/apache/beam/tree/master/sdks/java/io/iceberg/src/main/java/org/apache/beam/sdk/io/iceberg)
* [IcebergIO Javadoc](https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/iceberg/IcebergIO.html)
* [Apache Iceberg spec](https://iceberg.apache.org/spec/)
* [Apache Iceberg terms](https://iceberg.apache.org/terms/)
{{< /paragraph >}}


## Quickstart with Public Datasets

We can jump straight into reading some high-quality public datasets served via Iceberg's REST Catalog.
These datasets are hosted on Google Cloud's BigLake and are available to read by anyone, making it a good
resource to experiment with.

There are some prerequisites to using the BigLake Catalog:
- A Google Cloud Project (for authentication). Create an account [here](https://docs.cloud.google.com/docs/get-started) if you don't have one.
- Standard Google [Application Default Credentials](https://docs.cloud.google.com/docs/authentication/set-up-adc-local-dev-environment#local-user-cred) (ADC) set up in your environment.
- A [Google Cloud Storage bucket](https://docs.cloud.google.com/storage/docs/creating-buckets)

When you've met those prerequisites, start by setting up your catalog:
{{< highlight sql>}}
  CREATE CATALOG my_catalog TYPE 'iceberg'
  PROPERTIES (
    'type' = 'rest',
    'uri' = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    'warehouse' = 'gs://biglake-public-nyc-taxi-iceberg',
    'header.x-goog-user-project' = '$PROJECT_ID',
    'rest.auth.type' = 'google',
    'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO',
    'header.X-Iceberg-Access-Delegation' = 'vended-credentials'
  );
{{< /highlight >}}
{{< highlight java>}}
  {{< code_sample "examples/java/iceberg/src/main/java/org/apache/beam/examples/iceberg/snippets/Quickstart.java" biglake_public_catalog_props >}}
{{< /highlight >}}
{{< highlight py >}}
  {{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" biglake_public_catalog_props >}}
{{< /highlight >}}
{{< highlight yaml >}}
catalog_props: &catalog_props
  type: "rest"
  uri: "https://biglake.googleapis.com/iceberg/v1/restcatalog"
  warehouse: "gs://biglake-public-nyc-taxi-iceberg"
  header.x-goog-user-project: *PROJECT_ID
  rest.auth.type: "google"
  io-impl: "org.apache.iceberg.gcp.gcs.GCSFileIO"
  header.X-Iceberg-Access-Delegation: "vended-credentials"
{{< /highlight >}}

Now simply query the public dataset:
{{< highlight sql>}}
  SELECT
    passenger_count,
    COUNT(1) AS num_trips,
    ROUND(AVG(total_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance
  FROM
    bqms.public_data.nyc_taxicab
  WHERE
    data_file_year = 2021
    AND tip_amount > 100
  GROUP BY
    passenger_count
  ORDER BY
    num_trips DESC;
{{< /highlight >}}
{{< highlight java>}}
  {{< code_sample "examples/java/iceberg/src/main/java/org/apache/beam/examples/iceberg/snippets/Quickstart.java" biglake_public_query >}}
{{< /highlight >}}
{{< highlight py >}}
  {{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" biglake_public_query >}}
{{< /highlight >}}
{{< highlight yaml >}}
  pipeline:
    type: chain
    transforms:
      - type: ReadFromIceberg
        config:
        table: "public_data.nyc_taxicab"
        catalog_properties: *biglake_catalog_props
        filter: "data_file_year = 2021 AND tip_amount > 100"
        keep: [ "passenger_count", "total_amount", "trip_distance" ]
      - type: Sql
        config:
          query: "SELECT
                    passenger_count,
                    COUNT(1) AS num_trips,
                    ROUND(AVG(total_amount), 2) AS avg_fare,
                    ROUND(AVG(trip_distance), 2) AS avg_distance
                  FROM
                    PCOLLECTION
                  GROUP BY
                    passenger_count"
  {{< /highlight >}}

## User Guide

### Choose Your Catalog

First, select a Catalog implementation to handle metadata management and storage interaction.
Beam supports a wide variety of Iceberg catalogs, but this guide focuses on two common paths:
**Hadoop** for easy local development and **BigLake** for managing production data at cloud scale.

{{< tab hadoop >}}
  <p>
    Use Hadoop Catalog for quick, local testing with zero setup and no external dependencies.
    The following examples use a temporary local directory.
  </p>

  <br/>
  {{< highlight sql >}}
    CREATE CATALOG my_catalog TYPE 'iceberg'
    PROPERTIES (
      'type' = 'hadoop',
      'warehouse' = 'file:///tmp/beam-iceberg-local-quickstart',
    );
  {{< /highlight >}}
  {{< highlight java>}}
    {{< code_sample "examples/java/iceberg/src/main/java/org/apache/beam/examples/iceberg/snippets/Quickstart.java" hadoop_catalog_props >}}
  {{< /highlight >}}
  {{< highlight py >}}
    {{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" hadoop_catalog_config >}}
  {{< /highlight >}}
  {{< highlight yaml >}}
    catalog_props: &catalog_props
      type: "hadoop"
      warehouse: "file:///tmp/beam-iceberg-local-quickstart"
  {{< /highlight >}}
{{< /tab >}}
{{< tab BigLake >}}
{{% section %}}
Use BigLake Catalog for a fully managed REST-based experience. It simplifies access to cloud storage with
built-in credential delegation and unified metadata management. It requires a few pre-requisites:

- A Google Cloud Project (for authentication). Create an account [here](https://docs.cloud.google.com/docs/get-started) if you don't have one.
- Standard Google [Application Default Credentials](https://docs.cloud.google.com/docs/authentication/set-up-adc-local-dev-environment#local-user-cred) (ADC) set up in your environment.
- A [Google Cloud Storage bucket](https://docs.cloud.google.com/storage/docs/creating-buckets)

{{% /section %}}
  {{< highlight sql>}}
  CREATE CATALOG my_catalog TYPE 'iceberg'
  PROPERTIES (
    'type' = 'rest',
    'uri' = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    'warehouse' = 'gs://$BUCKET_NAME',
    'header.x-goog-user-project' = '$PROJECT_ID',
    'rest.auth.type' = 'google',
    'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO',
    'header.X-Iceberg-Access-Delegation' = 'vended-credentials'
  );
  {{< /highlight >}}
  {{< highlight java>}}
  {{< code_sample "examples/java/iceberg/src/main/java/org/apache/beam/examples/iceberg/snippets/Quickstart.java" biglake_catalog_props >}}
  {{< /highlight >}}
  {{< highlight py >}}
  {{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" biglake_catalog_props >}}
  {{< /highlight >}}
  {{< highlight yaml >}}
  catalog_props: &catalog_props
    type: "rest"
    uri: "https://biglake.googleapis.com/iceberg/v1/restcatalog"
    warehouse: "gs://" + *BUCKET_NAME
    header.x-goog-user-project: *PROJECT_ID
    rest.auth.type: "google"
    io-impl: "org.apache.iceberg.gcp.gcs.GCSFileIO"
    header.X-Iceberg-Access-Delegation: "vended-credentials"
  {{< /highlight >}}
{{< /tab >}}

### Create a Namespace

If you're on Beam SQL, you can explicitly create a new namespace:
```sql
CREATE DATABASE my_catalog.my_db;
```

Alternatively, the IcebergIO sink will automatically create missing namespaces at runtime.
This is ideal for dynamic pipelines where destinations are determined by the incoming data

### Create a Table
Tables are defined by a schema and an optional partition spec.
You can create a table using SQL DDL or by configuring the Iceberg destination in your Beam pipeline.

{{< highlight sql>}}
CREATE EXTERNAL TABLE my_catalog.my_db.my_table (
    id BIGINT,
    name VARCHAR,
    age INTEGER
)
TYPE 'iceberg'
{{< /highlight >}}
{{< highlight java>}}
{{< code_sample "examples/java/iceberg/src/main/java/org/apache/beam/examples/iceberg/snippets/Quickstart.java" managed_iceberg_config >}}
{{< /highlight >}}
{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" managed_iceberg_config >}}
{{< /highlight >}}
{{< highlight yaml >}}
- type: WriteToIceberg
  config:
    table: "my_db.my_table"
    catalog_properties: *catalog_props

# Note: The table will get created when inserting data (see below)
{{< /highlight >}}

### Insert Data
Once your table is defined, you can write data using standard SQL `INSERT` or by calling the IcebergIO sink in your SDK of choice.


{{< highlight sql>}}
INSERT INTO my_catalog.my_db.my_table VALUES
    (1, 'Mark', 32),
    (2, 'Omar', 24),
    (3, 'Rachel', 27);
{{< /highlight >}}
{{< highlight java>}}
{{< code_sample "examples/java/iceberg/src/main/java/org/apache/beam/examples/iceberg/snippets/Quickstart.java" managed_iceberg_insert >}}
{{< /highlight >}}
{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" managed_iceberg_insert >}}
{{< /highlight >}}
{{< highlight yaml >}}
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements:
          - id: 1
            name: "Mark"
            age: 32
          - id: 2
            name: "Omar"
            age: 24
          - id: 3
            name: "Rachel"
            age: 27
    - type: WriteToIceberg
      config:
        table: "my_db.my_table"
        catalog_properties: *catalog_props
{{< /highlight >}}

### View Namespaces and Tables

If you're on Beam SQL, you can view the newly created resources:
```sql
SHOW DATABASES my_catalog;
```
```sql
SHOW TABLES my_catalog.my_db;
```

### Query Data

{{< highlight sql>}}
SELECT * FROM my_catalog.my_db.my_table;
{{< /highlight >}}
{{< highlight java>}}
{{< code_sample "examples/java/iceberg/src/main/java/org/apache/beam/examples/iceberg/snippets/Quickstart.java" managed_iceberg_read >}}
{{< /highlight >}}
{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" managed_iceberg_read >}}
{{< /highlight >}}
{{< highlight yaml >}}
pipeline:
  type: chain
  transforms:
    - type: ReadFromIceberg
      config:
        table: "my_db.my_table"
        catalog_properties: *catalog_props
    - type: LogForTesting
{{< /highlight >}}

## Further steps

Check out the full [IcebergIO configuration](https://beam.apache.org/documentation/io/managed-io/#iceberg-write) to make
use of other features like applying a partition spec, table properties, row filtering, column pruning, etc.

## Data Types

Check this [overview of Iceberg data types](https://iceberg.apache.org/spec/#schemas-and-data-types).

IcebergIO leverages Beam Schemas to bridge the gap between SDK-native types and the Iceberg specification.
While the Java SDK provides full coverage for the Iceberg v2 spec (with v3 support currently in development),
other SDKs may have specific constraints on complex or experimental types. The following examples demonstrate
the standard mapping for core data types across SQL, Java, Python, and YAML:

{{< highlight sql >}}
  INSERT INTO catalog.namespace.table VALUES (
    9223372036854775807, -- BIGINT
    2147483647,          -- INTEGER
    1.0,                 -- FLOAT
    1.0,                 -- DOUBLE
    TRUE,                -- BOOLEAN
    TIMESTAMP '2018-05-28 20:17:40.123', -- TIMESTAMP
    'varchar',           -- VARCHAR
    'char',              -- CHAR
    ARRAY['abc', 'xyz'],  -- ARRAY
    ARRAY[CAST(ROW('abc', 123) AS ROW(nested_str VARCHAR, nested_int INTEGER))] -- ARRAY[STRUCT]
  )
{{< /highlight >}}
{{< highlight java >}}
  {{< code_sample "examples/java/iceberg/src/main/java/org/apache/beam/examples/iceberg/snippets/IcebergBeamSchemaAndRow.java" iceberg_schema_and_row >}}
{{< /highlight >}}
{{< highlight py >}}
  {{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" model_managed_iceberg_data_types >}}
{{< /highlight >}}
{{< highlight yaml >}}
  pipeline:
    transforms:
      - type: Create
        config:
          elements:
            - boolean_field: false
              integer_field: 123
              number_field: 4.56
              string_field: "abc"
              struct_field:
                nested_1: a
                nested_2: 1
              array_field: [1, 2, 3]
          output_schema:
            type: object
            properties:
              boolean_field:
                type: boolean
              integer_field:
                type: integer
              number_field:
                type: number
              string_field:
                type: string
              struct_field:
                type: object
                properties:
                  nested_1:
                    type: string
                  nested_2:
                    type: integer
              array_field:
                type: array
                items:
                  type: integer
{{< /highlight >}}

## Iceberg basics

### Catalogs

A catalog is a top-level entity used to manage and access Iceberg tables. There are many catalog implementations out there;
this guide focuses on the Hadoop catalog for easy local testing and BigLake REST catalog for cloud-scale development.

### Namespaces

A namespace lives inside a catalog and may contain a number of Iceberg tables. This is the equivalent of a "database".

### Tables

The actual entity containing data, and is described by a schema and partition spec.

### Snapshots

A new snapshot is created whenever a change is made to an Iceberg table. Each snapshot provides a summary of the change
and references its parent snapshot. An Iceberg table's history is a chronological list of snapshots, enabling features
like time travel and ACID-compliant concurrent writes.
