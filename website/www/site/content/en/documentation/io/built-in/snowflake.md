---
title: "Apache Snowflake I/O connector"
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

# Snowflake I/O
Pipeline options and general information about using and running Snowflake IO.

## Authentication
All authentication methods available for the Snowflake JDBC Driver are possible to use with the IO transforms:

- Username and password
- Key pair
- OAuth token

Passing credentials is done via Pipeline options.

Passing credentials is done via Pipeline options used to instantiate `SnowflakeIO.DataSourceConfiguration`:
{{< highlight java >}}
SnowflakePipelineOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(SnowflakePipelineOptions.class);
SnowflakeCredentials credentials = SnowflakeCredentialsFactory.of(options);

SnowflakeIO.DataSourceConfiguration.create(credentials)
        .(other DataSourceConfiguration options)
{{< /highlight >}}
### Username and password 
To use username/password authentication in SnowflakeIO, invoke your pipeline with the following Pipeline options:
{{< highlight >}}
--username=<USERNAME> --password=<PASSWORD>
{{< /highlight >}}
### Key pair
To use this authentication method, you must first generate a key pair and associate the public key with the Snowflake user that will connect using the IO transform. For instructions, see the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/jdbc-configure.html).

To use key pair authentication with SnowflakeIO, invoke your pipeline with following Pipeline options:
{{< highlight >}}
--username=<USERNAME> --privateKeyPath=<PATH_TO_P8_FILE> --privateKeyPassphrase=<PASSWORD_FOR_KEY>
{{< /highlight >}}

### OAuth token
SnowflakeIO also supports OAuth token.

**IMPORTANT**: SnowflakeIO requires a valid OAuth access token. It will neither be able to refresh the token nor obtain it using a web-based flow. For information on configuring an OAuth integration and obtaining the token, see the  [Snowflake documentation](https://docs.snowflake.com/en/user-guide/oauth-intro.html).

Once you have the token, invoke your pipeline with following Pipeline Options:
{{< highlight >}}
--oauthToken=<TOKEN>
{{< /highlight >}}
## DataSource Configuration
DataSource configuration is required in both read and write object for configuring Snowflake connection properties for IO purposes.
### General usage
Create the DataSource configuration:
{{< highlight java >}}
 SnowflakeIO.DataSourceConfiguration
            .create(SnowflakeCredentialsFactory.of(options))
            .withUrl(options.getUrl())
            .withServerName(options.getServerName())
            .withDatabase(options.getDatabase())
            .withWarehouse(options.getWarehouse())
            .withSchema(options.getSchema());
{{< /highlight >}}
Where parameters can be:

- ` .withUrl(...)`
  - JDBC-like URL for your Snowflake account, including account name and region, without any parameters.
  - Example: `.withUrl("jdbc:snowflake://account.snowflakecomputing.com")`
- `.withServerName(...)`
  - Server Name - full server name with account, zone and domain.
  - Example: `.withServerName("account.snowflakecomputing.com")`
- `.withDatabase(...)`
  - Name of the Snowflake database to use. 
  - Example: `.withDatabase("MY_DATABASE")`
- `.withWarehouse(...)`
  - Name of the Snowflake warehouse to use. This parameter is optional. If no warehouse name is specified, the default warehouse for the user is used.
  - Example: `.withWarehouse("MY_WAREHOUSE")`
- `.withSchema(...)`
  - Name of the schema in the database to use. This parameter is optional.
  - Example: `.withSchema("PUBLIC")`


**Note** - either `.withUrl(...)` or `.withServerName(...)` **is required**.

## Pipeline options
Use Beam’s [Pipeline options](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/options/PipelineOptions.html) to set options via the command line.
### Snowflake Pipeline options
Snowflake IO library supports following options that can be passed via the [command line](https://beam.apache.org/documentation/io/built-in/snowflake/#running-main-command-with-pipeline-options) by default when a Pipeline uses them:

`--url` Snowflake's JDBC-like url including account name and region without any parameters.

`--serverName` Full server name with account, zone and domain.

`--username` Required for username/password and Private Key authentication.

`--oauthToken` Required for OAuth authentication only.

`--password` Required for username/password authentication only.

`--privateKeyPath` Path to Private Key file. Required for Private Key authentication only.

`--privateKeyPassphrase` Private Key's passphrase. Required for Private Key authentication only.

`--stagingBucketName` External bucket path ending with `/`. I.e. `gs://bucket/`. Sub-directories are allowed.

`--storageIntegrationName` Storage integration name

`--warehouse` Warehouse to use. Optional.

`--database` Database name to connect to. Optional.

`--schema` Schema to use. Optional.

`--table` Table to use. Optional.

`--query` Query to use. Optional.

`--role` Role to use. Optional.

`--authenticator` Authenticator to use. Optional.

`--portNumber` Port number. Optional.

`--loginTimeout` Login timeout. Optional.

## Running pipelines on Dataflow
By default, pipelines are run on [Direct Runner](https://beam.apache.org/documentation/runners/direct/) on your local machine. To run a pipeline on [Google Dataflow](https://cloud.google.com/dataflow/), you must provide the following Pipeline options:

- `--runner=DataflowRunner`
  - The Dataflow’s specific runner.

- `--project=<GCS PROJECT>`
  - Name of the Google Cloud Platform project.

- `--stagingBucketName=<GCS BUCKET NAME>`
  - Google Cloud Services bucket where the Beam files will be staged.

- `--maxNumWorkers=5`
  - (optional) Maximum number of workers.

- `--appName=<JOB NAME>`
  - (optional) Prefix for the job name in the Dataflow Dashboard.

More pipeline options for Dataflow can be found [here](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/runners/dataflow/options/DataflowPipelineOptions.html).

**Note**: To properly authenticate with Google Cloud, please use [gcloud](https://cloud.google.com/sdk/gcloud/) or follow the [Google Cloud documentation](https://cloud.google.com/docs/authentication/).

**Important**: Please acknowledge [Google Dataflow pricing](Important: Please acknowledge Google Dataflow pricing).

## Writing to Snowflake tables
One of the functions of SnowflakeIO is writing to Snowflake tables. This transformation enables you to finish the Beam pipeline with an output operation that sends the user's [PCollection](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/values/PCollection.html) to your Snowflake database.
### Batch write (from a bounded source)
The basic .`write()` operation usage is as follows:
{{< highlight java >}}
data.apply(
   SnowflakeIO.<type>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .withStagingBucketName("BUCKET NAME")
       .withStorageIntegrationName("STORAGE INTEGRATION NAME")
       .withUserDataMapper(mapper)
)
{{< /highlight >}}
Replace type with the data type of the PCollection object to write; for example, SnowflakeIO.<String> for an input PCollection of Strings.

All the below parameters are required:

- `.withDataSourceConfiguration()` Accepts a DatasourceConfiguration object.

- `.to()` Accepts the target Snowflake table name.

- `.withStagingBucketName()` Accepts a cloud bucket path ended with slash.
 -Example: `.withStagingBucketName("gs://mybucket/my/dir/")`

- `.withStorageIntegrationName()` Accepts a name of a Snowflake storage integration object created according to Snowflake documentationt. Example:
{{< highlight >}}
CREATE OR REPLACE STORAGE INTEGRATION test_integration 
TYPE = EXTERNAL_STAGE 
STORAGE_PROVIDER = GCS 
ENABLED = TRUE 
STORAGE_ALLOWED_LOCATIONS = ('gcs://bucket/');
{{< /highlight >}}
Then:
{{< highlight >}}
.withStorageIntegrationName(test_integration)
{{< /highlight >}}

- `.withUserDataMapper()` Accepts the UserDataMapper function that will map a user's PCollection to an array of String values `(String[])`.

**Note**:
SnowflakeIO uses COPY statements behind the scenes to write (using [COPY to table](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html)). StagingBucketName will be used to save CSV files which will end up in Snowflake. Those CSV files will be saved under the “stagingBucketName” path.

### UserDataMapper function
The UserDataMapper function is required to map data from a PCollection to an array of String values before the `write()` operation saves the data to temporary .csv files. For example:
{{< highlight java >}}
public static SnowflakeIO.UserDataMapper<Long> getCsvMapper() {
    return (SnowflakeIO.UserDataMapper<Long>) recordLine -> new String[] {recordLine.toString()};
}
{{< /highlight >}}

### Additional write options
#### Transformation query
The `.withQueryTransformation()` option for the `write()` operation accepts a SQL query as a String value, which will be performed while transfering data staged in CSV files directly to the target Snowflake table. For information about the transformation SQL syntax,  see the [Snowflake Documentation](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html#transformation-parameters).

Usage:
{{< highlight java >}}
String query = "SELECT t.$1 from YOUR_TABLE;";
data.apply(
   SnowflakeIO.<~>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .withStagingBucketName("BUCKET NAME")
       .withStorageIntegrationName("STORAGE INTEGRATION NAME")
       .withUserDataMapper(mapper)
       .withQueryTransformation(query)
)
{{< /highlight >}}

#### Write disposition
Define the write behaviour based on the table where data will be written to by specifying the `.withWriteDisposition(...)` option for the `write()` operation. The following values are supported:

- APPEND - Default behaviour. Written data is added to the existing rows in the table,

- EMPTY - The target table must be empty;  otherwise, the write operation fails,

- TRUNCATE - The write operation deletes all rows from the target table before writing to it.

Example of usage:
{{< highlight java >}}
data.apply(
   SnowflakeIO.<~>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .withStagingBucketName("BUCKET NAME")
       .withStorageIntegrationName("STORAGE INTEGRATION NAME")
       .withUserDataMapper(mapper)
       .withWriteDisposition(TRUNCATE)
)
{{< /highlight >}}

#### Create disposition
The `.withCreateDisposition()` option defines the behavior of the write operation if the target table does not exist . The following values are supported:

- CREATE_IF_NEEDED - default behaviour. The write operation checks whether the specified target table exists; if it does not, the write operation attempts to create the table Specify the schema for the target table using the `.withTableSchema()` option.

- CREATE_NEVER -  The write operation fails if the target table does not exist.

Usage:
{{< highlight java >}}
data.apply(
   SnowflakeIO.<~>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .withStagingBucketName("BUCKET NAME")
       .withStorageIntegrationName("STORAGE INTEGRATION NAME")
       .withUserDataMapper(mapper)
       .withCreateDisposition(CREATE_NEVER)
)
{{< /highlight >}}

#### Table schema disposition
When the `.withCreateDisposition()` .option is set to `CREATE_IF_NEEDED`, the `.withTableSchema()` option enables specifying the schema for the created target table. 
A table schema is a list of `SFColumn` objects with name and type corresponding to column type for each column in the table. 

Usage:
{{< highlight java >}}
SFTableSchema tableSchema =
    new SFTableSchema(
        SFColumn.of("my_date", new SFDate(), true),
        new SFColumn("id", new SFNumber()),
        SFColumn.of("name", new SFText(), true));

data.apply(
   SnowflakeIO.<~>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .withStagingBucketName("BUCKET NAME")
       .withStorageIntegrationName("STORAGE INTEGRATION NAME")
       .withUserDataMapper(mapper)
       .withTableSchema(tableSchema)
)
{{< /highlight >}}
## Reading from Snowflake
One of the functions of SnowflakeIO is reading Snowflake tables - either full tables via table name or custom data via query. Output of the read transform is a [PCollection](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/values/PCollection.html) of user-defined data type.

### General usage

The basic `.read()` operation usage:
{{< highlight java >}}
PCollection<USER_DATA_TYPE> items = pipeline.apply(
   SnowflakeIO.<USER_DATA_TYPE>read()
       .withDataSourceConfiguration(dc)
       .fromTable("MY_TABLE") // or .fromQuery("QUERY")
       .withStagingBucketName("BUCKET NAME")
       .withStorageIntegrationName("STORAGE INTEGRATION NAME")
       .withCsvMapper(mapper)
       .withCoder(coder));
)
{{< /highlight >}}
Where all below parameters are required:

- `.withDataSourceConfiguration(...)`
  - Accepts a DataSourceConfiguration object.

- `.fromTable(...) or .fromQuery(...)`
  - Specifies a Snowflake table name or custom SQL query.

- `.withStagingBucketName()`
  - Accepts a cloud bucket name.

-  `.withStorageIntegrationName()`
  - Accepts a name of a Snowflake storage integration object created according to Snowflake documentation. Example:
{{< highlight >}}
CREATE OR REPLACE STORAGE INTEGRATION test_integration 
TYPE = EXTERNAL_STAGE 
STORAGE_PROVIDER = GCS 
ENABLED = TRUE 
STORAGE_ALLOWED_LOCATIONS = ('gcs://bucket/');
{{< /highlight >}}
Then:
{{< highlight >}}
.withStorageIntegrationName(test_integration)
{{< /highlight >}}

- `.withCsvMapper(mapper)`
  - Accepts a [CSVMapper](https://beam.apache.org/documentation/io/built-in/snowflake/#csvmapper) instance for mapping String[] to USER_DATA_TYPE.
- `.withCoder(coder)`
  - Accepts the [Coder](https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/coders/Coder.html) for USER_DATA_TYPE.

**Note**:
SnowflakeIO uses COPY statements behind the scenes to read (using [COPY to location](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html)) files staged in cloud storage.StagingBucketName will be used as a temporary location for storing CSV files. Those temporary directories will be named `sf_copy_csv_DATE_TIME_RANDOMSUFFIX` and they will be removed automatically once Read operation finishes.

### CSVMapper
SnowflakeIO uses a [COPY INTO <location>](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html) statement to move data from a Snowflake table to Google Cloud Storage as CSV files. These files are then downloaded via [FileIO](https://beam.apache.org/releases/javadoc/2.3.0/index.html?org/apache/beam/sdk/io/FileIO.html) and processed line by line. Each line is split into an array of Strings using the [OpenCSV](http://opencsv.sourceforge.net/) library. 

The CSVMapper’s job is to give the user the possibility to convert the array of Strings to a user-defined type, ie. GenericRecord for Avro or Parquet files, or custom POJO.

Example implementation of CsvMapper for GenericRecord:
{{< highlight java >}}
static SnowflakeIO.CsvMapper<GenericRecord> getCsvMapper() {
   return (SnowflakeIO.CsvMapper<GenericRecord>)
           parts -> {
               return new GenericRecordBuilder(PARQUET_SCHEMA)
                       .set("ID", Long.valueOf(parts[0]))
                       .set("NAME", parts[1])
                       [...]
                       .build();
           };
}
{{< /highlight >}}