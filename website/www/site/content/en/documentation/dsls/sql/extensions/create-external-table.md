---
type: languages
title: "Beam SQL extension: CREATE EXTERNAL TABLE Statement"
aliases:
  - /documentation/dsls/sql/create-external-table/
  - /documentation/dsls/sql/statements/create-table/
  - /documentation/dsls/sql/create-table/
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

# Beam SQL extensions: CREATE EXTERNAL TABLE

Beam SQL's `CREATE EXTERNAL TABLE` statement registers a virtual table that maps to an
[external storage system](/documentation/io/built-in/).
For some storage systems, `CREATE EXTERNAL TABLE` does not create a physical table until
a write occurs. After the physical table exists, you can access the table with
the `SELECT`, `JOIN`, and `INSERT INTO` statements.

The `CREATE EXTERNAL TABLE` statement includes a schema and extended clauses.

## Syntax

```
CREATE EXTERNAL TABLE [ IF NOT EXISTS ] tableName (tableElement [, tableElement ]*)
TYPE type
[LOCATION location]
[TBLPROPERTIES tblProperties]

simpleType: TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | DECIMAL | BOOLEAN | DATE | TIME | TIMESTAMP | CHAR | VARCHAR

fieldType: simpleType | MAP<simpleType, fieldType> | ARRAY<fieldType> | ROW<tableElement [, tableElement ]*>

tableElement: columnName fieldType [ NOT NULL ]
```

*   `IF NOT EXISTS`: Optional. If the table is already registered, Beam SQL
    ignores the statement instead of returning an error.
*   `tableName`: The case sensitive name of the table to create and register,
    specified as an
    [Identifier](/documentation/dsls/sql/calcite/lexical#identifiers).
    The table name does not need to match the name in the underlying data
    storage system.
*   `tableElement`: `columnName` `fieldType` `[ NOT NULL ]`
    *   `columnName`: The case sensitive name of the column, specified as a
        backtick_quoted_expression.
    *   `fieldType`: The field's type, specified as one of the following types:
        *   `simpleType`: `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`,
            `DOUBLE`, `DECIMAL`, `BOOLEAN`, `DATE`, `TIME`, `TIMESTAMP`, `CHAR`,
            `VARCHAR`
        *   `MAP<simpleType, fieldType>`
        *   `ARRAY<fieldType>`
        *   `ROW<tableElement [, tableElement ]*>`
    *   `NOT NULL`: Optional. Indicates that the column is not nullable.
*   `type`: The I/O transform that backs the virtual table, specified as an
    [Identifier](/documentation/dsls/sql/calcite/lexical/#identifiers)
    with one of the following values:
    *   `bigquery`
    *   `pubsub`
    *   `kafka`
    *   `text`
*   `location`: The I/O specific location of the underlying table, specified as
    a [String
    Literal](/documentation/dsls/sql/calcite/lexical/#string-literals).
    See the I/O specific sections for `location` format requirements.
*   `tblProperties`: The I/O specific quoted key value JSON object with extra
    configuration, specified as a [String
    Literal](/documentation/dsls/sql/calcite/lexical/#string-literals).
    See the I/O specific sections for `tblProperties` format requirements.

## BigQuery

### Syntax

```
CREATE EXTERNAL TABLE [ IF NOT EXISTS ] tableName (tableElement [, tableElement ]*)
TYPE bigquery
LOCATION '[PROJECT_ID]:[DATASET].[TABLE]'
TBLPROPERTIES '{"method": "DIRECT_READ"}'
```

*   `LOCATION`: Location of the table in the BigQuery CLI format.
    *   `PROJECT_ID`: ID of the Google Cloud Project.
    *   `DATASET`: BigQuery Dataset ID.
    *   `TABLE`: BigQuery Table ID within the Dataset.
*   `TBLPROPERTIES`:
    *   `method`: Optional. Read method to use. Following options are available:
        *   `DIRECT_READ`: Use the BigQuery Storage API.
        *   `EXPORT`: Export data to Google Cloud Storage in Avro format and read data files from that location.
        *   Default is `DIRECT_READ` for Beam 2.21+ (older versions use `EXPORT`).

### Read Mode

Beam SQL supports reading columns with simple types (`simpleType`) and arrays of simple
types (`ARRAY<simpleType>`).

When reading using `EXPORT` method the following pipeline options should be set:
*   `project`: ID of the Google Cloud Project.
*   `tempLocation`: Bucket to store intermediate data in. Ex: `gs://temp-storage/temp`.

When reading using `DIRECT_READ` method, an optimizer will attempt to perform
project and predicate push-down, potentially reducing the time requited to read the data from BigQuery.

More information about the BigQuery Storage API can be found [here](https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-api).

### Write Mode

if the table does not exist, Beam creates the table specified in location when
the first record is written. If the table does exist, the specified columns must
match the existing table.

### Schema

Schema-related errors will cause the pipeline to crash. The Map type is not
supported. Beam SQL types map to [BigQuery Standard SQL
types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
as follows:

<table>
  <tr>
   <td>Beam SQL Type
   </td>
   <td>BigQuery Standard SQL Type
   </td>
  </tr>
  <tr>
   <td>TINYINT, SMALLINT, INTEGER, BIGINT &nbsp;
   </td>
   <td>INT64
   </td>
  </tr>
  <tr>
   <td>FLOAT, DOUBLE, DECIMAL
   </td>
   <td>FLOAT64
   </td>
  </tr>
  <tr>
   <td>BOOLEAN
   </td>
   <td>BOOL
   </td>
  </tr>
  <tr>
   <td>DATE
   </td>
   <td>DATE
   </td>
  </tr>
  <tr>
   <td>TIME
   </td>
   <td>TIME
   </td>
  </tr>
  <tr>
   <td>TIMESTAMP
   </td>
   <td>TIMESTAMP
   </td>
  </tr>
  <tr>
   <td>CHAR, VARCHAR
   </td>
   <td>STRING
   </td>
  </tr>
  <tr>
   <td>MAP
   </td>
   <td>(not supported)
   </td>
  </tr>
  <tr>
   <td>ARRAY
   </td>
   <td>ARRAY
   </td>
  </tr>
  <tr>
   <td>ROW
   </td>
   <td>STRUCT
   </td>
  </tr>
</table>

### Example

```
CREATE EXTERNAL TABLE users (id INTEGER, username VARCHAR)
TYPE bigquery
LOCATION 'testing-integration:apache.users'
```

## Pub/Sub

### Syntax

```
CREATE EXTERNAL TABLE [ IF NOT EXISTS ] tableName
  (
   event_timestamp TIMESTAMP,
   attributes MAP<VARCHAR, VARCHAR>,
   payload ROW<tableElement [, tableElement ]*>
  )
TYPE pubsub
LOCATION 'projects/[PROJECT]/topics/[TOPIC]'
TBLPROPERTIES '{"timestampAttributeKey": "key", "deadLetterQueue": "projects/[PROJECT]/topics/[TOPIC]"}'
```

*   `event_timestamp`: The event timestamp associated with the Pub/Sub message
    by PubsubIO. It can be one of the following:
    *   Message publish time, which is provided by Pub/Sub. This is the default
        value if no extra configuration is provided.
    *   A timestamp specified in one of the user-provided message attributes.
        The attribute key is configured by the `timestampAttributeKey` field of
        the `tblProperties` blob. The value of the attribute should conform to
        the [requirements of
        PubsubIO](https://beam.apache.org/releases/javadoc/2.4.0/org/apache/beam/sdk/io/gcp/pubsub/PubsubIO.Read.html#withTimestampAttribute-java.lang.String-),
        which is either millis since Unix epoch or [RFC 339
        ](https://www.ietf.org/rfc/rfc3339.txt)date string.
*   `attributes`: The user-provided attributes map from the Pub/Sub message;
*   `payload`: The schema of the JSON payload of the Pub/Sub message. No other
    payload formats are currently supported by Beam SQL. If a record can't be
    unmarshalled, the record is written to the topic specified in the
    `deadLeaderQueue` field of the `tblProperties` blob. If no dead-letter queue
    is specified in this case, an exception is thrown and the pipeline will
    crash.
*   `LOCATION`:
    *   `PROJECT`: ID of the Google Cloud Project
    *   `TOPIC`: The Pub/Sub topic name. A subscription will be created
        automatically, but the subscription is not cleaned up automatically.
        Specifying an existing subscription is not supported.
*   `TBLPROPERTIES`:
    *   `timestampAttributeKey`: Optional. The key which contains the event
        timestamp associated with the Pub/Sub message. If not specified, the
        message publish timestamp is used as an event timestamp for
        windowing/watermarking.
    *   `deadLetterQueue`: The topic into which messages are written if the
        payload was not parsed. If not specified, an exception is thrown for
        parsing failures.

### Read Mode

PubsubIO is currently limited to read access only.

### Write Mode

Not supported. PubSubIO is currently limited to read access only in Beam SQL.

### Schema

Pub/Sub messages have metadata associated with them, and you can reference this
metadata in your queries. For each message, Pub/Sub exposes its publish time and
a map of user-provided attributes in addition to the payload (unstructured in
the general case). This information must be preserved and accessible from the
SQL statements. Currently, this means that PubsubIO tables require you to
declare a special set of columns, as shown below.

### Supported Payload

*   JSON Objects
    *   Beam only supports querying messages with payload containing JSON
        objects. Beam attempts to parse JSON to match the schema of the
        `payload` field.

### Example

```
CREATE EXTERNAL TABLE locations (event_timestamp TIMESTAMP, attributes MAP<VARCHAR, VARCHAR>, payload ROW<id INTEGER, location VARCHAR>)
TYPE pubsub
LOCATION 'projects/testing-integration/topics/user-location'
```

## Kafka

KafkaIO is experimental in Beam SQL.

### Syntax

```
CREATE EXTERNAL TABLE [ IF NOT EXISTS ] tableName (tableElement [, tableElement ]*)
TYPE kafka
LOCATION 'kafka://localhost:2181/brokers'
TBLPROPERTIES '{"bootstrap.servers":"localhost:9092", "topics": ["topic1", "topic2"]}'
```

*   `LOCATION`: The Kafka topic URL.
*   `TBLPROPERTIES`:
    *   `bootstrap.servers`: Optional. Allows you to specify the bootstrap
        server.
    *   `topics`: Optional. Allows you to specify specific topics.

### Read Mode

Read Mode supports reading from a topic.

### Write Mode

Write Mode supports writing to a topic.

### Supported Payload

*   CSV
    *   Beam parses the messages, attempting to parse fields according to the
        types specified in the schema.

### Schema

Only simple types are supported.

## MongoDB

### Syntax

```
CREATE EXTERNAL TABLE [ IF NOT EXISTS ] tableName (tableElement [, tableElement ]*)
TYPE mongodb
LOCATION 'mongodb://[HOST]:[PORT]/[DATABASE]/[COLLECTION]'
```
*   `LOCATION`: Location of the collection.
    *   `HOST`: Location of the MongoDB server. Can be localhost or an ip address.
         When authentication is required username and password can be specified
         as follows: `username:password@localhost`.
    *   `PORT`: Port on which MongoDB server is listening.
    *   `DATABASE`: Database to connect to.
    *   `COLLECTION`: Collection within the database.

### Read Mode

Read Mode supports reading from a collection.

### Write Mode

Write Mode supports writing to a collection.

### Schema

Only simple types are supported. MongoDB documents are mapped to Beam SQL types via [`JsonToRow`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/JsonToRow.html) transform.

### Example

```
CREATE EXTERNAL TABLE users (id INTEGER, username VARCHAR)
TYPE mongodb
LOCATION 'mongodb://localhost:27017/apache/users'
```

## Text

TextIO is experimental in Beam SQL. Read Mode and Write Mode do not currently
access the same underlying data.

### Syntax

```
CREATE EXTERNAL TABLE [ IF NOT EXISTS ] tableName (tableElement [, tableElement ]*)
TYPE text
LOCATION '/home/admin/orders'
TBLPROPERTIES '{"format: "Excel"}'
```

*   `LOCATION`: The path to the file for Read Mode. The prefix for Write Mode.
*   `TBLPROPERTIES`:
    *   `format`: Optional. Allows you to specify the CSV Format, which controls
        the field delimeter, quote character, record separator, and other properties.
        See the following table:

{{< table class="table-bordered" >}}
| Value for `format` | Field delimiter | Quote | Record separator | Ignore empty lines? | Allow missing column names? |
|--------------------|-----------------|-------|------------------|---------------------|-----------------------------|
| `default`          | `,`             | `"`   | `\r\n`           | Yes                 | No                          |
| `rfc4180`          | `,`             | `"`   | `\r\n`           | No                  | No                          |
| `excel`            | `,`             | `"`   | `\r\n`           | No                  | Yes                         |
| `tdf`              | `\t`            | `"`   | `\r\n`           | Yes                 | No                          |
| `mysql`            | `\t`            | none  | `\n`             | No                  | No                          |
{{< /table >}}

### Read Mode

Read Mode supports reading from a file.

### Write Mode

Write Mode supports writing to a set of files. TextIO creates file on writes.

### Supported Payload

*   CSV
    *   Beam parses the messages, attempting to parse fields according to the
        types specified in the schema using org.apache.commons.csv.

### Schema

Only simple types are supported.

### Example

```
CREATE EXTERNAL TABLE orders (id INTEGER, price INTEGER)
TYPE text
LOCATION '/home/admin/orders'
```
