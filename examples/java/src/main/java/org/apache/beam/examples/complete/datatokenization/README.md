<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache Beam pipeline example to tokenize data using remote RPC server

This directory contains an Apache Beam example that creates a pipeline to read data from one of
the supported sources, tokenize data with external API calls to remote RPC server, and write data into one of the supported sinks.

Supported data formats:

- JSON
- CSV

Supported input sources:

- Local file system
- [Google Cloud Storage](https://cloud.google.com/storage)
- [Google Pub/Sub](https://cloud.google.com/pubsub)

Supported destination sinks:

- Local file system
- Google Cloud Storage
- [Google Cloud BigQuery](https://cloud.google.com/bigquery)
- [Cloud BigTable](https://cloud.google.com/bigtable)

Supported data schema format:

- JSON with an array of fields described in BigQuery format

In the main scenario, the template will create an Apache Beam pipeline that will read data in CSV or
JSON format from a specified input source, send the data to an external processing server, receive
processed data, and write it into a specified output sink.

## Requirements

- Java 8
- 1 of supported sources to read data from
- 1 of supported destination sinks to write data into
- A configured RPC to tokenize data

## Getting Started

This section describes what is needed to get the template up and running.

- Gradle preparation
- Local execution
- Running as a Dataflow Template
    - Setting Up Project Environment
    - Build Data Tokenization Dataflow Flex Template
    - Creating the Dataflow Flex Template
    - Executing Template
    

## Gradle preparation

To run this example your `build.gradle` file should contain the following task to execute the pipeline:

```
task execute (type:JavaExec) {
    main = System.getProperty("mainClass")
    classpath = sourceSets.main.runtimeClasspath
    systemProperties System.getProperties()
    args System.getProperty("exec.args", "").split()
}
```

This task allows to run the pipeline via the following command:

```bash
gradle clean execute -DmainClass=org.apache.beam.examples.complete.datatokenization.DataTokenization \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

## Running the pipeline

To execute this pipeline, specify the parameters:

**NOTE:** Any parameters called as GSC also supports local file system.

- Data schema
    - **dataSchemaGcsPath**: Path to data schema (JSON format) in GCS compatible with BigQuery.
- 1 specified input source out of these:
    - Google Cloud Storage
        - **inputGcsFilePattern**: GCS filepattern for files in bucket to read data from
        - **inputGcsFileFormat**: File format of input files. Supported formats: JSON, CSV
        - In case if input data is in CSV format:
            - **csvContainsHeaders**: `true` if file(s) in bucket to read data from contain headers,
              and `false` otherwise
            - **csvDelimiter**: Delimiting character in CSV. Default: use delimiter provided in
              csvFormat
            - **csvFormat**: Csv format according to Apache Commons CSV format. Default is:
              [Apache Commons CSV default](https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.html#DEFAULT)
              . Must match format names exactly found
              at: https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.Predefined.html
    - Google Pub/Sub
        - **pubsubTopic**: The Cloud Pub/Sub topic to read from, in the format of '
          projects/yourproject/topics/yourtopic'
- 1 specified output sink out of these:
    - Google Cloud Storage
        - **outputGcsDirectory**: GCS directory in bucket to write data to
        - **outputGcsFileFormat**: File format of output files. Supported formats: JSON, CSV
        - **windowDuration**: The window duration in which data will be written. Should be specified
          only for 'Pub/Sub -> GCS' case. Defaults to 30s.

          Allowed formats are:
            - Ns (for seconds, example: 5s),
            - Nm (for minutes, example: 12m),
            - Nh (for hours, example: 2h).
        - Google Cloud BigQuery
            - **bigQueryTableName**: Cloud BigQuery table name to write into
    - Cloud BigTable
        - **bigTableProjectId**: Id of the project where the Cloud BigTable instance to write into
          is located
        - **bigTableInstanceId**: Id of the Cloud BigTable instance to write into
        - **bigTableTableId**: Id of the Cloud BigTable table to write into
        - **bigTableKeyColumnName**: Column name to use as a key in Cloud BigTable
        - **bigTableColumnFamilyName**: Column family name to use in Cloud BigTable
- DSG parameters
    - **dsgUri**: URI for the API calls to DSG
    - **batchSize**: Size of the batch to send to DSG per request
    - **payloadConfigGcsPath**: GCS path to the payload configuration file with an array of fields
      to extract for tokenization

The template allows for the user to supply the following optional parameter:

- **nonTokenizedDeadLetterGcsPath**: GCS folder where failed to tokenize data will be stored


in the following format:

```bash
--dataSchemaGcsPath="path-to-data-schema-in-json-format"
--inputGcsFilePattern="path-patternt-to-input-data"
--outputGcsDirectory="path-to-output-directory"
#example to usage csv
--inputGcsFileFormat="CSV"
--outputGcsFileFormat="CSV"
--csvContainsHeaders="true"
--nonTokenizedDeadLetterGcsPath="path-to-errors-rows-writing"
--batchSize=batch-size-number
--rpcUri=http://host:port/tokenize
```

By default, this will run the pipeline locally with the DirectRunner. To change the runner, specify:

```bash
--runner=YOUR_SELECTED_RUNNER
```

See the [documentation](http://beam.apache.org/get-started/quickstart/) and
the [Examples README](../../../../../../../../../README.md) for more information about how to run this example.


## Running as a Dataflow Template


## Running as a Dataflow Template

This example also exists as Google Dataflow Template, which you can build and run using Google Cloud Platform. See
this template documentation [README.md](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/master/v2/protegrity-data-tokenization/README.md) for
more information.
