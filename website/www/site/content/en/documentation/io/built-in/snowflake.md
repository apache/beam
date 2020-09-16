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
To use this authentication method, you must first generate a key pair and associate the public key with the Snowflake user that will connect using the IO transform. For instructions,  see the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/jdbc-configure.html).

To use key pair authentication with SnowflakeIO, invoke your pipeline with one of the following set of Pipeline options:
{{< highlight >}}
--username=<USERNAME> --privateKeyPath=<PATH_TO_P8_FILE> --privateKeyPassphrase=<PASSWORD_FOR_KEY>
--username=<USERNAME> --rawPrivateKey=<PRIVATE_KEY> --privateKeyPassphrase=<PASSWORD_FOR_KEY>
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
            .create()
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
- `.withUsernamePasswordAuth(username, password)`
  - Sets username/password authentication.
  - Example: `.withUsernamePasswordAuth("USERNAME", "PASSWORD")`
- `.withOAuth(token)`
  - Sets OAuth authentication.
  - Example: `.withOAuth("TOKEN")`
- `.withKeyPairAuth(username, privateKey)`
  - Sets key pair authentication using username and [PrivateKey](https://docs.oracle.com/javase/8/docs/api/java/security/PrivateKey.html)
  - Example: `.withKeyPairAuth("USERNAME",` [PrivateKey](https://docs.oracle.com/javase/8/docs/api/java/security/PrivateKey.html)`)`
- `.withKeyPairPathAuth(username, privateKeyPath, privateKeyPassphrase)`
  - Sets key pair authentication using username, path to private key file and passphrase.
  - Example: `.withKeyPairPathAuth("USERNAME", "PATH/TO/KEY.P8", "PASSPHRASE")`
- `.withKeyPairRawAuth(username, rawPrivateKey, privateKeyPassphrase)`
  - Sets key pair authentication using username, private key and passphrase.
  - Example: `.withKeyPairRawAuth("USERNAME", "PRIVATE_KEY", "PASSPHRASE")`


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

`--rawPrivateKey` Private Key. Required for Private Key authentication only.

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

`--snowPipe` SnowPipe name. Optional.

### Running main command with Pipeline options
To pass Pipeline options via the command line, use `--args` in a gradle command as follows:

{{< highlight >}}
./gradle run
    --args="
        --serverName=<SNOWFLAKE SERVER NAME>
           Example: --serverName=account.region.gcp.snowflakecomputing.com
        --username=<SNOWFLAKE USERNAME>
           Example: --username=testuser
        --password=<SNOWFLAKE PASSWORD>
           Example: --password=mypassword
        --database=<SNOWFLAKE DATABASE>
           Example: --database=TEST_DATABASE
        --schema=<SNOWFLAKE SCHEMA>
           Example: --schema=public
        --table=<SNOWFLAKE TABLE IN DATABASE>
           Example: --table=TEST_TABLE
        --query=<IF NOT TABLE THEN QUERY>
           Example: --query=‘SELECT column FROM TABLE’
        --storageIntegrationName=<SNOWFLAKE STORAGE INTEGRATION NAME>
           Example: --storageIntegrationName=my_integration
        --stagingBucketName=<GCS BUCKET NAME>
           Example: --stagingBucketName=gs://my_gcp_bucket/
        --runner=<DirectRunner/DataflowRunner>
           Example: --runner=DataflowRunner
        --project=<FOR DATAFLOW RUNNER: GCP PROJECT NAME>
           Example: --project=my_project
        --tempLocation=<FOR DATAFLOW RUNNER: GCS TEMP LOCATION STARTING
                        WITH gs://…>
           Example: --tempLocation=gs://my_bucket/temp/
        --region=<FOR DATAFLOW RUNNER: GCP REGION>
           Example: --region=us-east-1
        --appName=<OPTIONAL: DATAFLOW JOB NAME PREFIX>
           Example: --appName=my_job"
{{< /highlight >}}
Then in the code it is possible to access the parameters with arguments using the options.getStagingBucketName(); command.

### Running test command with Pipeline options
To pass Pipeline options via the command line, use `-DintegrationTestPipelineOptions` in a gradle command as follows:
{{< highlight >}}
./gradlew test --tests nameOfTest
-DintegrationTestPipelineOptions='[
  "--serverName=<SNOWFLAKE SERVER NAME>",
      Example: --serverName=account.region.gcp.snowflakecomputing.com
  "--username=<SNOWFLAKE USERNAME>",
      Example: --username=testuser
  "--password=<SNOWFLAKE PASSWORD>",
      Example: --password=mypassword
  "--schema=<SNOWFLAKE SCHEMA>",
      Example: --schema=PUBLIC
  "--table=<SNOWFLAKE TABLE IN DATABASE>",
      Example: --table=TEST_TABLE
  "--database=<SNOWFLAKE DATABASE>",
      Example: --database=TEST_DATABASE
  "--storageIntegrationName=<SNOWFLAKE STORAGE INTEGRATION NAME>",
      Example: --storageIntegrationName=my_integration
  "--stagingBucketName=<GCS BUCKET NAME>",
      Example: --stagingBucketName=gs://my_gcp_bucket
  "--externalLocation=<GCS BUCKET URL STARTING WITH GS://>",
      Example: --tempLocation=gs://my_bucket/temp/
]' --no-build-cache
{{< /highlight >}}

Where all parameters are starting with “--”, they are surrounded with double quotation and separated with comma:

- `--serverName=<SNOWFLAKE SERVER NAME>`
  - Specifies the full name of your account (provided by Snowflake). Note that your full account name might include additional segments that identify the region and cloud platform where your account is hosted.
  - Example: `--serverName=xy12345.eu-west-1.gcp..snowflakecomputing.com`

- `--username=<SNOWFLAKE USERNAME>`
  - Specifies the login name of the user.
  - Example: `--username=my_username`

- `--password=<SNOWFLAKE PASSWORD>`
  - Specifies the password for the specified user.
  - Example: `--password=my_secret`

- `--schema=<SNOWFLAKE SCHEMA>`
  - Specifies the schema to use for the specified database once connected. The specified schema should be an existing schema for which the specified user’s role has privileges.
  - Example: `--schema=PUBLIC`

- `--table=<SNOWFLAKE TABLE IN DATABASE>`
  - Example: `--table=MY_TABLE`

- `--database=<SNOWFLAKE DATABASE>`
  - Specifies the database to use once connected. The specified database should be an existing database for which the specified user’s role has privileges.
  - Example: `--database=MY_DATABASE`

- `--storageIntegrationName=<SNOWFLAKE STORAGE INTEGRATION NAME>`
  - Name of storage integration created in [Snowflake](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html) for a cloud storage of choice.
  - Example: `--storageIntegrationName=my_google_integration`

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

**Important**: Please acknowledge [Google Dataflow pricing](https://cloud.google.com/dataflow/pricing)

### Running pipeline templates on Dataflow
Google Dataflow is supporting [template](https://cloud.google.com/dataflow/docs/guides/templates/overview) creation which means staging pipelines on Cloud Storage and running them with ability to pass runtime parameters that are only available during pipeline execution.

The process of creating own Dataflow template is following
1. Create your own pipeline.
2. Create [Dataflow template](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates#creating-and-staging-templates) with checking which options SnowflakeIO is supporting at runtime.
3. Run a Dataflow template using [Cloud Console](https://cloud.google.com/dataflow/docs/guides/templates/running-templates#using-the-cloud-console), [REST API](https://cloud.google.com/dataflow/docs/guides/templates/running-templates#using-the-rest-api) or [gcloud](https://cloud.google.com/dataflow/docs/guides/templates/running-templates#using-gcloud).

Currently, SnowflakeIO supports following options at runtime:

- `--serverName` Full server name with account, zone and domain.

- `--username` Required for username/password and Private Key authentication.

- `--password` Required for username/password authentication only.

- `--rawPrivateKey` Private Key file. Required for Private Key authentication only.

- `--privateKeyPassphrase` Private Key's passphrase. Required for Private Key authentication only.

- `--stagingBucketName` external bucket path ending with `/`. I.e. `gs://bucket/`. Sub-directories are allowed.

- `--storageIntegrationName` Storage integration name.

- `--warehouse` Warehouse to use. Optional.

- `--database` Database name to connect to. Optional.

- `--schema` Schema to use. Optional.

- `--table` Table to use. Optional. Note: table is not in default pipeline options.

- `--query` Query to use. Optional. Note: query is not in default pipeline options.

- `--role` Role to use. Optional.

- `--snowPipe` SnowPipe name. Optional.

**Note**: table and query is not in pipeline options by default, it may be added by [extending](https://beam.apache.org/documentation/io/built-in/snowflake/#extending-pipeline-options) PipelineOptions.

Currently, SnowflakeIO **doesn't support** following options at runtime:

- `--url` Snowflake's JDBC-like url including account name and region without any parameters.

- `--oauthToken` Required for OAuth authentication only.

- `--privateKeyPath` Path to Private Key file. Required for Private Key authentication only.

- `--authenticator` Authenticator to use. Optional.

- `--portNumber` Port number. Optional.

- `--loginTimeout` Login timeout. Optional.

## Writing to Snowflake tables
One of the functions of SnowflakeIO is writing to Snowflake tables. This transformation enables you to finish the Beam pipeline with an output operation that sends the user's [PCollection](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/values/PCollection.html) to your Snowflake database.
### Batch write (from a bounded source)
The basic .`write()` operation usage is as follows:
{{< highlight java >}}
data.apply(
   SnowflakeIO.<type>write()
       .withDataSourceConfiguration(dc)
       .toTable("MY_TABLE")
       .withStagingBucketName("BUCKET NAME")
       .withStorageIntegrationName("STORAGE INTEGRATION NAME")
       .withUserDataMapper(mapper)
)
{{< /highlight >}}
Replace type with the data type of the PCollection object to write; for example, SnowflakeIO.<String> for an input PCollection of Strings.

All the below parameters are required:

- `.withDataSourceConfiguration()` Accepts a DatasourceConfiguration object.

- `.toTable()` Accepts the target Snowflake table name.

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

**Optional** for batching:
- `.withQuotationMark()`
  - Default value: `‘` (single quotation mark).
  - Accepts String with one character. It will surround all text (String) fields saved to CSV. It should be one of the accepted characters by [Snowflake’s](https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html) [FIELD_OPTIONALLY_ENCLOSED_BY](https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html) parameter (double quotation mark, single quotation mark or none).
  - Example: `.withQuotationMark("'")`
### Streaming write  (from unbounded source)
It is required to create a [SnowPipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe.html) in the Snowflake console. SnowPipe should use the same integration and the same bucket as specified by .withStagingBucketName and .withStorageIntegrationName methods. The write operation might look as follows:
{{< highlight java >}}
data.apply(
   SnowflakeIO.<type>write()
      .withStagingBucketName("BUCKET NAME")
      .withStorageIntegrationName("STORAGE INTEGRATION NAME")
      .withDataSourceConfiguration(dc)
      .withUserDataMapper(mapper)
      .withSnowPipe("MY_SNOW_PIPE")
      .withFlushTimeLimit(Duration.millis(time))
      .withFlushRowLimit(rowsNumber)
      .withShardsNumber(shardsNumber)
)
{{< /highlight >}}
#### Parameters
**Required** for streaming:

- ` .withDataSourceConfiguration()`
  - Accepts a DatasourceConfiguration object.

- `.toTable()`
  - Accepts the target Snowflake table name.
  - Example: `.toTable("MY_TABLE)`

- `.withStagingBucketName()`
  - Accepts a cloud bucket path ended with slash.
  - Example: `.withStagingBucketName("gs://mybucket/my/dir/")`

- `.withStorageIntegrationName()`
  - Accepts a name of a Snowflake storage integration object created according to Snowflake documentationt.
  - Example:
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

- `.withSnowPipe()`
  - Accepts the target SnowPipe name. `.withSnowPipe()` accepts the exact name of snowpipe.
Example:
{{< highlight >}}
CREATE OR REPLACE PIPE test_database.public.test_gcs_pipe
AS COPY INTO stream_table from @streamstage;
{{< /highlight >}}

   - Then:
{{< highlight >}}
.withSnowPipe(test_gcs_pipe)
{{< /highlight >}}

**Note**: this is important to provide **schema** and **database** names.
- `.withUserDataMapper()`
  - Accepts the [UserDataMapper](https://beam.apache.org/documentation/io/built-in/snowflake/#userdatamapper-function) function that will map a user's PCollection to an array of String values `(String[]).`

**Note**:

SnowflakeIO uses COPY statements behind the scenes to write (using [COPY to table](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html)). StagingBucketName will be used to save CSV files which will end up in Snowflake. Those CSV files will be saved under the “stagingBucketName” path.

**Optional** for streaming:
- `.withFlushTimeLimit()`
  - Default value: 30 seconds
  - Accepts Duration objects with the specified time after each the streaming write will be repeated
  - Example: `.withFlushTimeLimit(Duration.millis(180000))`

- `.withFlushRowLimit()`
  - Default value: 10,000 rows
  - Limit of rows written to each file staged file
  - Example: `.withFlushRowLimit(500000)`

- `.withShardNumber()`
  - Default value: 1 shard
  - Number of files that will be saved in every flush (for purposes of parallel write).
  - Example: `.withShardNumber(5)`

- `.withQuotationMark()`
  - Default value: `‘` (single quotation mark).
  - Accepts String with one character. It will surround all text (String) fields saved to CSV. It should be one of the accepted characters by [Snowflake’s](https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html) [FIELD_OPTIONALLY_ENCLOSED_BY](https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html) parameter (double quotation mark, single quotation mark or none). Example: .withQuotationMark("") (no quotation marks)

- `.withDebugMode()`
  - Accepts:
    - `SnowflakeIO.StreamingLogLevel.INFO` - shows whole info about loaded files
    - `SnowflakeIO.StreamingLogLevel.ERROR` - shows only errors.
  - Shows logs about streamed files to Snowflake similarly to [insertReport](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis.html#endpoint-insertreport). Enabling debug mode may influence performance.
  - Example: `.withDebugMode(SnowflakeIO.StreamingLogLevel.INFO)`



**Important notice**: Streaming accepts only **key pair authentication**.

#### Flush time: duration & number of rows
Duration: streaming write will write periodically files on stage according to time duration specified in flush time limit (for example. every 1 minute).

Number of rows: files staged for write will have number of rows specified in flush row limit unless the flush time limit will be reached (for example if the limit is 1000 rows and buffor collected 99 rows and the 1 minute flush time passes, the rows will be sent to SnowPipe for insertion).

Size of staged files will depend on the rows size and used compression (GZIP).

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
       .toTable("MY_TABLE")
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
       .toTable("MY_TABLE")
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
       .toTable("MY_TABLE")
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
       .toTable("MY_TABLE")
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
- `withCoder(coder)`
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
## Using SnowflakeIO in Python SDK
### Intro
Snowflake cross-language implementation is supporting both reading and writing operations for Python programming language, thanks to
cross-language which is part of [Portability Framework Roadmap](https://beam.apache.org/roadmap/portability/) which aims to provide full interoperability
across the Beam ecosystem. From a developer perspective it means the possibility of combining transforms written in different languages(Java/Python/Go).

Currently, cross-language is supporting only [Apache Flink](https://flink.apache.org/) as a runner in a stable manner but plans are to support all runners.
For more information about cross-language please see [multi sdk efforts](https://beam.apache.org/roadmap/connectors-multi-sdk/)
and [Beam on top of Flink](https://flink.apache.org/ecosystem/2020/02/22/apache-beam-how-beam-runs-on-top-of-flink.html) articles.

### Set up
Please see [Apache Beam with Flink runner](https://beam.apache.org/documentation/runners/flink/) for a setup.

### Reading from Snowflake
One of the functions of SnowflakeIO is reading Snowflake tables - either full tables via table name or custom data via query. Output of the read transform is a [PCollection](https://beam.apache.org/releases/pydoc/2.20.0/apache_beam.pvalue.html#apache_beam.pvalue.PCollection) of user-defined data type.
#### General usage
{{< highlight >}}
OPTIONS = [
   "--runner=FlinkRunner",
   "--flink_version=1.10",
   "--flink_master=localhost:8081",
   "--environment_type=LOOPBACK"
]

with TestPipeline(options=PipelineOptions(OPTIONS)) as p:
   (p
       | ReadFromSnowflake(
           server_name=<SNOWFLAKE SERVER NAME>,
           username=<SNOWFLAKE USERNAME>,
           password=<SNOWFLAKE PASSWORD>,
           o_auth_token=<OAUTH TOKEN>,
           private_key_path=<PATH TO P8 FILE>,
           raw_private_key=<PRIVATE_KEY>
           private_key_passphrase=<PASSWORD FOR KEY>,
           schema=<SNOWFLAKE SCHEMA>,
           database=<SNOWFLAKE DATABASE>,
           staging_bucket_name=<GCS BUCKET NAME>,
           storage_integration_name=<SNOWFLAKE STORAGE INTEGRATION NAME>,
           csv_mapper=<CSV MAPPER FUNCTION>,
           table=<SNOWFALKE TABLE>,
           query=<IF NOT TABLE THEN QUERY>,
           role=<SNOWFLAKE ROLE>,
           warehouse=<SNOWFLAKE WAREHOUSE>,
           expansion_service=<EXPANSION SERVICE ADDRESS>))
{{< /highlight >}}

#### Required parameters
- `server_name` Full Snowflake server name with an account, zone, and domain.

- `schema` Name of the Snowflake schema in the database to use.

- `database` Name of the Snowflake database to use.

- `staging_bucket_name` Name of the Google Cloud Storage bucket. Bucket will be used as a temporary location for storing CSV files. Those temporary directories will be named `sf_copy_csv_DATE_TIME_RANDOMSUFFIX` and they will be removed automatically once Read operation finishes.

- `storage_integration_name` Is the name of a Snowflake storage integration object created according to [Snowflake documentation](https://docs.snowflake.net/manuals/sql-reference/sql/create-storage-integration.html).

- `csv_mapper` Specifies a function which must translate user-defined object to array of strings. SnowflakeIO uses a [COPY INTO <location>](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html) statement to move data from a Snowflake table to Google Cloud Storage as CSV files. These files are then downloaded via [FileIO](https://beam.apache.org/releases/javadoc/2.3.0/index.html?org/apache/beam/sdk/io/FileIO.html) and processed line by line. Each line is split into an array of Strings using the [OpenCSV](http://opencsv.sourceforge.net/) library. The csv_mapper function job is to give the user the possibility to convert the array of Strings to a user-defined type, ie. GenericRecord for Avro or Parquet files, or custom objects.
Example:
{{< highlight >}}
def csv_mapper(strings_array):
    return User(strings_array[0], int(strings_array[1])))
{{< /highlight >}}

- `table or query` Specifies a Snowflake table name or custom SQL query

- `expansion_service`: specifies URL of expansion service.

#### Authentication parameters
It’s required to pass one of the following combinations of valid parameters for authentication:
- `username and password` Specifies username and password for username/password authentication method.

- `private_key_path and private_key_passphrase` Specifies a path to private key and passphrase for key/pair authentication method.

- `raw_private_key and private_key_passphrase` Specifies a private key and passphrase for key/pair authentication method.

- `o_auth_token` Specifies access token for OAuth authentication method.

#### Additional parameters
- `role`: specifies Snowflake role. If not specified then the user's default will be used.

- `warehouse`: specifies Snowflake warehouse name. If not specified then the user's default will be used.

### Writing to Snowflake
One of the functions of SnowflakeIO is writing to Snowflake tables. This transformation enables you to finish the Beam pipeline with an output operation that sends the user's [PCollection](https://beam.apache.org/releases/pydoc/2.20.0/apache_beam.pvalue.html#apache_beam.pvalue.PCollection) to your Snowflake database.
#### General usage
{{< highlight >}}
OPTIONS = [
   "--runner=FlinkRunner",
   "--flink_version=1.10",
   "--flink_master=localhost:8081",
   "--environment_type=LOOPBACK"
]

with TestPipeline(options=PipelineOptions(OPTIONS)) as p:
   (p
       | <SOURCE OF DATA>
       | WriteToSnowflake(
           server_name=<SNOWFLAKE SERVER NAME>,
           username=<SNOWFLAKE USERNAME>,
           password=<SNOWFLAKE PASSWORD>,
           o_auth_token=<OAUTH TOKEN>,
           private_key_path=<PATH TO P8 FILE>,
           raw_private_key=<PRIVATE_KEY>
           private_key_passphrase=<PASSWORD FOR KEY>,
           schema=<SNOWFLAKE SCHEMA>,
           database=<SNOWFLAKE DATABASE>,
           staging_bucket_name=<GCS BUCKET NAME>,
           storage_integration_name=<SNOWFLAKE STORAGE INTEGRATION NAME>,
           create_disposition=<CREATE DISPOSITION>,
           write_disposition=<WRITE DISPOSITION>,
           table_schema=<SNOWFLAKE TABLE SCHEMA>,
           user_data_mapper=<USER DATA MAPPER FUNCTION>,
           table=<SNOWFALKE TABLE>,
           query=<IF NOT TABLE THEN QUERY>,
           role=<SNOWFLAKE ROLE>,
           warehouse=<SNOWFLAKE WAREHOUSE>,
           expansion_service=<EXPANSION SERVICE ADDRESS>))
{{< /highlight >}}
#### Required parameters

- `server_name` Full Snowflake server name with account, zone and domain.

- `schema` Name of the Snowflake schema in the database to use.

- `database` Name of the Snowflake database to use.

- `staging_bucket_name` Path to Google Cloud Storage bucket ended with slash. Bucket will be used to save CSV files which will end up in Snowflake. Those CSV files will be saved under “staging_bucket_name” path.

- `storage_integration_name` Is the name of a Snowflake storage integration object created according to [Snowflake documentation](https://docs.snowflake.net/manuals/sql-reference/sql/create-storage-integration.html).

- `user_data_mapper` Specifies a function which  maps data from a PCollection to an array of String values before the write operation saves the data to temporary .csv files.
Example:
{{< highlight >}}
def user_data_mapper(user):
    return [user.name, str(user.age)]
{{< /highlight >}}

- `table or query` Specifies a Snowflake table name or custom SQL query

- `expansion_service` Specifies URL of expansion service.

#### Authentication parameters
It’s required to pass one of the following combination of valid parameters for authentication:

- `username and password` Specifies username/password authentication method.

- `private_key_path and private_key_passphrase` Specifies a path to private key and passphrase for key/pair authentication method.

- `raw_private_key and private_key_passphrase` Specifies a private key and passphrase for key/pair authentication method.

- `o_auth_token` Specifies access token for OAuth authentication method.

#### Additional parameters
- `role`: specifies Snowflake role. If not specified then the user's default will be used.

- `warehouse`: specifies Snowflake warehouse name. If not specified then the user's default will be used.

- `create_disposition` Defines the behaviour of the write operation if the target table does not exist. The following values are supported:
  - CREATE_IF_NEEDED - default behaviour. The write operation checks whether the specified target table exists; if it does not, the write operation attempts to create the table Specify the schema for the target table using the table_schema parameter.
  - CREATE_NEVER -  The write operation fails if the target table does not exist.

- `write_disposition` Defines the write behaviour based on the table where data will be written to. The following values are supported:
  - APPEND - Default behaviour. Written data is added to the existing rows in the table,
  - EMPTY - The target table must be empty;  otherwise, the write operation fails,
  - TRUNCATE - The write operation deletes all rows from the target table before writing to it.

- `table_schema` When the create_disposition parameter is set to CREATE_IF_NEEDED, the table_schema  parameter  enables specifying the schema for the created target table. A table schema is as JSON with the following structure:
{{< highlight >}}
{"schema":[
    {
      "dataType":{"type":"<COLUMN DATA TYPE>"},
      "name":"<COLUMN  NAME> ",
      "nullable": <NULLABLE>
    },
        ...
  ]}
{{< /highlight >}}
All supported data types:
{{< highlight >}}
{"dataType":{"type":"date"},"name":"","nullable":false},
{"dataType":{"type":"datetime"},"name":"","nullable":false},
{"dataType":{"type":"time"},"name":"","nullable":false},
{"dataType":{"type":"timestamp"},"name":"","nullable":false},
{"dataType":{"type":"timestamp_ltz"},"name":"","nullable":false},
{"dataType":{"type":"timestamp_ntz"},"name":"","nullable":false},
{"dataType":{"type":"timestamp_tz"},"name":"","nullable":false},
{"dataType":{"type":"boolean"},"name":"","nullable":false},
{"dataType":{"type":"decimal","precision":38,"scale":1},"name":"","nullable":true},
{"dataType":{"type":"double"},"name":"","nullable":false},
{"dataType":{"type":"float"},"name":"","nullable":false},
{"dataType":{"type":"integer","precision":38,"scale":0},"name":"","nullable":false},
{"dataType":{"type":"number","precision":38,"scale":1},"name":"","nullable":false},
{"dataType":{"type":"numeric","precision":40,"scale":2},"name":"","nullable":false},
{"dataType":{"type":"real"},"name":"","nullable":false},
{"dataType":{"type":"array"},"name":"","nullable":false},
{"dataType":{"type":"object"},"name":"","nullable":false},
{"dataType":{"type":"variant"},"name":"","nullable":true},
{"dataType":{"type":"binary","size":null},"name":"","nullable":false},
{"dataType":{"type":"char","length":1},"name":"","nullable":false},
{"dataType":{"type":"string","length":null},"name":"","nullable":false},
{"dataType":{"type":"text","length":null},"name":"","nullable":false},
{"dataType":{"type":"varbinary","size":null},"name":"","nullable":false},
{"dataType":{"type":"varchar","length":100},"name":"","nullable":false}]
{{< /highlight >}}
