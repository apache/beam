---
type: languages
title: "Beam SQL extension: CREATE CATALOG Statement"
aliases:
  - /documentation/dsls/sql/create-catalog/
  - /documentation/dsls/sql/statements/create-catalog/
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

# Beam SQL extensions: CREATE CATALOG

Beam SQL's `CREATE CATALOG` statement creates and registers a catalog that manages metadata for external data sources. Catalogs provide a unified interface for accessing different types of data stores and enable features like schema management, table discovery, and cross-catalog queries.

Currently, Beam SQL supports the **Apache Iceberg** catalog type, which provides access to Iceberg tables with full ACID transaction support, schema evolution, and time travel capabilities.

## Syntax

```
CREATE CATALOG [ IF NOT EXISTS ] catalogName
TYPE catalogType
[PROPERTIES (propertyKey = propertyValue [, propertyKey = propertyValue ]*)]
```

*   `IF NOT EXISTS`: Optional. If the catalog is already registered, Beam SQL
    ignores the statement instead of returning an error.
*   `catalogName`: The case sensitive name of the catalog to create and register,
    specified as an [Identifier](/documentation/dsls/sql/calcite/lexical#identifiers).
*   `catalogType`: The type of catalog to create. Currently supported values:
    *   `iceberg`: Apache Iceberg catalog
*   `PROPERTIES`: Optional. Key-value pairs for catalog-specific configuration.
    Each property is specified as `'key' = 'value'` with string literals.

## Apache Iceberg Catalog

The Iceberg catalog provides access to [Apache Iceberg](https://iceberg.apache.org/) tables, which are high-performance table formats for huge analytic datasets.

### Syntax

```
CREATE CATALOG [ IF NOT EXISTS ] catalogName
TYPE iceberg
PROPERTIES (
    'catalog-impl' = 'catalogImplementation',
    'warehouse' = 'warehouseLocation'
    [, additionalProperties...]
)
```

### Required Properties

*   `catalog-impl`: The Iceberg catalog implementation class. Common values:
    *   `org.apache.iceberg.hadoop.HadoopCatalog`: For Hadoop-compatible storage (HDFS, S3, GCS, etc.)
    *   `org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog`: For BigQuery integration
    *   `org.apache.iceberg.jdbc.JdbcCatalog`: For JDBC-based metadata storage
    *   `org.apache.iceberg.rest.RESTCatalog`: For REST-based catalog access
*   `warehouse`: The root location where Iceberg tables and metadata are stored.
    Format depends on the storage system:
    *   **Local filesystem**: `file:///path/to/warehouse`
    *   **HDFS**: `hdfs://namenode:port/path/to/warehouse`
    *   **S3**: `s3://bucket-name/path/to/warehouse`
    *   **Google Cloud Storage**: `gs://bucket-name/path/to/warehouse`

### Optional Properties

The available optional properties depend on the catalog implementation:

#### Hadoop Catalog Properties

*   `io-impl`: The file I/O implementation class. Common values:
    *   `org.apache.iceberg.hadoop.HadoopFileIO`: For Hadoop-compatible storage
    *   `org.apache.iceberg.aws.s3.S3FileIO`: For S3 storage
    *   `org.apache.iceberg.gcp.gcs.GCSFileIO`: For Google Cloud Storage
*   `hadoop.*`: Any Hadoop configuration property (e.g., `hadoop.fs.s3a.access.key`)

#### BigQuery Metastore Catalog Properties

*   `io-impl`: Must be `org.apache.iceberg.gcp.gcs.GCSFileIO` for GCS storage
*   `gcp_project`: Google Cloud Project ID
*   `gcp_region`: Google Cloud region (e.g., `us-central1`)
*   `gcp_location`: Alternative to `gcp_region` for specifying location

#### JDBC Catalog Properties

*   `uri`: JDBC connection URI
*   `jdbc.user`: Database username
*   `jdbc.password`: Database password
*   `jdbc.driver`: JDBC driver class name

### Examples

#### Hadoop Catalog with Local Storage

```sql
CREATE CATALOG my_iceberg_catalog
TYPE iceberg
PROPERTIES (
    'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',
    'warehouse' = 'file:///tmp/iceberg-warehouse'
)
```

#### Hadoop Catalog with S3 Storage

```sql
CREATE CATALOG s3_iceberg_catalog
TYPE iceberg
PROPERTIES (
    'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',
    'warehouse' = 's3://my-bucket/iceberg-warehouse',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    'hadoop.fs.s3a.access.key' = 'your-access-key',
    'hadoop.fs.s3a.secret.key' = 'your-secret-key'
)
```

#### BigQuery Metastore Catalog

```sql
CREATE CATALOG bigquery_iceberg_catalog
TYPE iceberg
PROPERTIES (
    'catalog-impl' = 'org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog',
    'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO',
    'warehouse' = 'gs://my-bucket/iceberg-warehouse',
    'gcp_project' = 'my-gcp-project',
    'gcp_region' = 'us-central1'
)
```

#### JDBC Catalog

```sql
CREATE CATALOG jdbc_iceberg_catalog
TYPE iceberg
PROPERTIES (
    'catalog-impl' = 'org.apache.iceberg.jdbc.JdbcCatalog',
    'uri' = 'jdbc:postgresql://localhost:5432/iceberg_metadata',
    'jdbc.user' = 'iceberg_user',
    'jdbc.password' = 'iceberg_password',
    'jdbc.driver' = 'org.postgresql.Driver',
    'warehouse' = 's3://my-bucket/iceberg-warehouse'
)
```

## Using Catalogs

After creating a catalog, you can use it to manage databases and tables:

### Switch to a Catalog

```sql
USE CATALOG catalogName
```

### Create and Use a Database

```sql
-- Create a database (namespace)
CREATE DATABASE my_database

-- Use the database
USE DATABASE my_database
```

### Create Tables in the Catalog

Once you've switched to a catalog and database, you can create tables:

```sql
-- Switch to your catalog and database
USE CATALOG my_iceberg_catalog
USE DATABASE my_database

-- Create an Iceberg table
CREATE EXTERNAL TABLE users (
    id BIGINT,
    username VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
)
TYPE iceberg
```

## Catalog Management

### List Available Catalogs

```sql
SHOW CATALOGS
```

### Drop a Catalog

```sql
DROP CATALOG [ IF EXISTS ] catalogName
```

## Best Practices

### Security

*   **Credentials**: Store sensitive credentials (access keys, passwords) in secure configuration systems rather than hardcoding them in SQL statements
*   **IAM Roles**: Use IAM roles and service accounts when possible instead of access keys
*   **Network Security**: Ensure proper network access controls for your storage systems

### Performance

*   **Warehouse Location**: Choose a warehouse location that's geographically close to your compute resources
*   **Partitioning**: Use appropriate partitioning strategies for your data access patterns
*   **File Formats**: Iceberg automatically manages file formats, but consider compression settings for your use case

### Monitoring

*   **Catalog Health**: Monitor catalog connectivity and performance
*   **Storage Usage**: Track warehouse storage usage and implement lifecycle policies
*   **Query Performance**: Monitor query performance and optimize table schemas as needed

## Troubleshooting

### Common Issues

#### Catalog Creation Fails

*   **Check Dependencies**: Ensure all required Iceberg dependencies are available in your classpath
*   **Verify Properties**: Double-check that all required properties are provided and correctly formatted
*   **Storage Access**: Ensure your compute environment has access to the specified warehouse location

#### Table Operations Fail

*   **Catalog Context**: Make sure you're using the correct catalog with `USE CATALOG`
*   **Database Context**: Ensure you're in the correct database with `USE DATABASE`
*   **Permissions**: Verify that your credentials have the necessary permissions for the storage system

#### Performance Issues

*   **Partitioning**: Review your table partitioning strategy
*   **File Size**: Check if files are too large or too small for your use case
*   **Compression**: Consider adjusting compression settings for your data types

### Getting Help

For more information about Apache Iceberg:

*   [Apache Iceberg Documentation](https://iceberg.apache.org/docs/)
*   [Iceberg Catalog Implementations](https://iceberg.apache.org/docs/latest/configuration/)
*   [Beam SQL Documentation](/documentation/dsls/sql/)
