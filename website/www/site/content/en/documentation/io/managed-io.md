---
title: "Managed I/O Connectors"
aliases: [built-in]
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

# Managed I/O Connectors

Beam’s new Managed API streamlines how you use existing I/Os, offering both
simplicity and powerful enhancements. I/Os are now configured through a
lightweight, consistent interface: a simple configuration map with a unified
API that spans multiple connectors.

With Managed I/O, runners gain deeper insight into each I/O’s structure and
intent. This allows the runner to optimize performance, adjust behavior
dynamically, or even replace the I/O with a more efficient or updated
implementation behind the scenes.

For example, the DataflowRunner can seamlessly upgrade a Managed transform to
its latest SDK version, automatically applying bug fixes and new features (no
manual updates or user intervention required!)

## Available Configurations

<i>Note: required configuration fields are <strong>bolded</strong>.</i>

<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Connector Name</th>
      <th>Read Configuration</th>
      <th>Write Configuration</th>
    </tr>
    <tr>
      <td><strong>ICEBERG_CDC</strong></td>
      <td>
        <strong>table</strong> (<code style="color: green">str</code>)<br>
        catalog_name (<code style="color: green">str</code>)<br>
        catalog_properties (<code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>)<br>
        config_properties (<code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>)<br>
        from_snapshot (<code style="color: #f54251">int64</code>)<br>
        from_timestamp (<code style="color: #f54251">int64</code>)<br>
        poll_interval_seconds (<code style="color: #f54251">int32</code>)<br>
        starting_strategy (<code style="color: green">str</code>)<br>
        streaming (<code style="color: orange">boolean</code>)<br>
        to_snapshot (<code style="color: #f54251">int64</code>)<br>
        to_timestamp (<code style="color: #f54251">int64</code>)<br>
      </td>
      <td>
        Unavailable
      </td>
    </tr>
    <tr>
      <td><strong>ICEBERG</strong></td>
      <td>
        <strong>table</strong> (<code style="color: green">str</code>)<br>
        catalog_name (<code style="color: green">str</code>)<br>
        catalog_properties (<code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>)<br>
        config_properties (<code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>)<br>
      </td>
      <td>
        <strong>table</strong> (<code style="color: green">str</code>)<br>
        catalog_name (<code style="color: green">str</code>)<br>
        catalog_properties (<code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>)<br>
        config_properties (<code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>)<br>
        drop (<code>list[<span style="color: green;">str</span>]</code>)<br>
        keep (<code>list[<span style="color: green;">str</span>]</code>)<br>
        only (<code style="color: green">str</code>)<br>
        triggering_frequency_seconds (<code style="color: #f54251">int32</code>)<br>
      </td>
    </tr>
    <tr>
      <td><strong>KAFKA</strong></td>
      <td>
        <strong>bootstrap_servers</strong> (<code style="color: green">str</code>)<br>
        <strong>topic</strong> (<code style="color: green">str</code>)<br>
        confluent_schema_registry_subject (<code style="color: green">str</code>)<br>
        confluent_schema_registry_url (<code style="color: green">str</code>)<br>
        consumer_config_updates (<code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>)<br>
        file_descriptor_path (<code style="color: green">str</code>)<br>
        format (<code style="color: green">str</code>)<br>
        message_name (<code style="color: green">str</code>)<br>
        schema (<code style="color: green">str</code>)<br>
      </td>
      <td>
        <strong>bootstrap_servers</strong> (<code style="color: green">str</code>)<br>
        <strong>format</strong> (<code style="color: green">str</code>)<br>
        <strong>topic</strong> (<code style="color: green">str</code>)<br>
        file_descriptor_path (<code style="color: green">str</code>)<br>
        message_name (<code style="color: green">str</code>)<br>
        producer_config_updates (<code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>)<br>
        schema (<code style="color: green">str</code>)<br>
      </td>
    </tr>
    <tr>
      <td><strong>BIGQUERY</strong></td>
      <td>
        kms_key (<code style="color: green">str</code>)<br>
        query (<code style="color: green">str</code>)<br>
        row_restriction (<code style="color: green">str</code>)<br>
        fields (<code>list[<span style="color: green;">str</span>]</code>)<br>
        table (<code style="color: green">str</code>)<br>
      </td>
      <td>
        <strong>table</strong> (<code style="color: green">str</code>)<br>
        drop (<code>list[<span style="color: green;">str</span>]</code>)<br>
        keep (<code>list[<span style="color: green;">str</span>]</code>)<br>
        kms_key (<code style="color: green">str</code>)<br>
        only (<code style="color: green">str</code>)<br>
        triggering_frequency_seconds (<code style="color: #f54251">int64</code>)<br>
      </td>
    </tr>
  </table>
</div>

## Configuration Details

### `ICEBERG_CDC` Read

<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Configuration</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
    <tr>
      <td>
        <strong>table</strong>
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        Identifier of the Iceberg table.
      </td>
    </tr>
    <tr>
      <td>
        catalog_name
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        Name of the catalog containing the table.
      </td>
    </tr>
    <tr>
      <td>
        catalog_properties
      </td>
      <td>
        <code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>
      </td>
      <td>
        Properties used to set up the Iceberg catalog.
      </td>
    </tr>
    <tr>
      <td>
        config_properties
      </td>
      <td>
        <code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>
      </td>
      <td>
        Properties passed to the Hadoop Configuration.
      </td>
    </tr>
    <tr>
      <td>
        from_snapshot
      </td>
      <td>
        <code style="color: #f54251">int64</code>
      </td>
      <td>
        Starts reading from this snapshot ID (inclusive).
      </td>
    </tr>
    <tr>
      <td>
        from_timestamp
      </td>
      <td>
        <code style="color: #f54251">int64</code>
      </td>
      <td>
        Starts reading from the first snapshot (inclusive) that was created after this timestamp (in milliseconds).
      </td>
    </tr>
    <tr>
      <td>
        poll_interval_seconds
      </td>
      <td>
        <code style="color: #f54251">int32</code>
      </td>
      <td>
        The interval at which to poll for new snapshots. Defaults to 60 seconds.
      </td>
    </tr>
    <tr>
      <td>
        starting_strategy
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The source's starting strategy. Valid options are: "earliest" or "latest". Can be overriden by setting a starting snapshot or timestamp. Defaults to earliest for batch, and latest for streaming.
      </td>
    </tr>
    <tr>
      <td>
        streaming
      </td>
      <td>
        <code style="color: orange">boolean</code>
      </td>
      <td>
        Enables streaming reads, where source continuously polls for snapshots forever.
      </td>
    </tr>
    <tr>
      <td>
        to_snapshot
      </td>
      <td>
        <code style="color: #f54251">int64</code>
      </td>
      <td>
        Reads up to this snapshot ID (inclusive).
      </td>
    </tr>
    <tr>
      <td>
        to_timestamp
      </td>
      <td>
        <code style="color: #f54251">int64</code>
      </td>
      <td>
        Reads up to the latest snapshot (inclusive) created before this timestamp (in milliseconds).
      </td>
    </tr>
  </table>
</div>

### `ICEBERG` Write

<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Configuration</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
    <tr>
      <td>
        <strong>table</strong>
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        Identifier of the Iceberg table.
      </td>
    </tr>
    <tr>
      <td>
        catalog_name
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        Name of the catalog containing the table.
      </td>
    </tr>
    <tr>
      <td>
        catalog_properties
      </td>
      <td>
        <code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>
      </td>
      <td>
        Properties used to set up the Iceberg catalog.
      </td>
    </tr>
    <tr>
      <td>
        config_properties
      </td>
      <td>
        <code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>
      </td>
      <td>
        Properties passed to the Hadoop Configuration.
      </td>
    </tr>
    <tr>
      <td>
        drop
      </td>
      <td>
        <code>list[<span style="color: green;">str</span>]</code>
      </td>
      <td>
        A list of field names to drop from the input record before writing. Is mutually exclusive with 'keep' and 'only'.
      </td>
    </tr>
    <tr>
      <td>
        keep
      </td>
      <td>
        <code>list[<span style="color: green;">str</span>]</code>
      </td>
      <td>
        A list of field names to keep in the input record. All other fields are dropped before writing. Is mutually exclusive with 'drop' and 'only'.
      </td>
    </tr>
    <tr>
      <td>
        only
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The name of a single record field that should be written. Is mutually exclusive with 'keep' and 'drop'.
      </td>
    </tr>
    <tr>
      <td>
        triggering_frequency_seconds
      </td>
      <td>
        <code style="color: #f54251">int32</code>
      </td>
      <td>
        For a streaming pipeline, sets the frequency at which snapshots are produced.
      </td>
    </tr>
  </table>
</div>

### `ICEBERG` Read

<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Configuration</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
    <tr>
      <td>
        <strong>table</strong>
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        Identifier of the Iceberg table.
      </td>
    </tr>
    <tr>
      <td>
        catalog_name
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        Name of the catalog containing the table.
      </td>
    </tr>
    <tr>
      <td>
        catalog_properties
      </td>
      <td>
        <code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>
      </td>
      <td>
        Properties used to set up the Iceberg catalog.
      </td>
    </tr>
    <tr>
      <td>
        config_properties
      </td>
      <td>
        <code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>
      </td>
      <td>
        Properties passed to the Hadoop Configuration.
      </td>
    </tr>
  </table>
</div>

### `KAFKA` Read

<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Configuration</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
    <tr>
      <td>
        <strong>bootstrap_servers</strong>
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form `host1:port1,host2:port2,...`
      </td>
    </tr>
    <tr>
      <td>
        <strong>topic</strong>
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        n/a
      </td>
    </tr>
    <tr>
      <td>
        confluent_schema_registry_subject
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        n/a
      </td>
    </tr>
    <tr>
      <td>
        confluent_schema_registry_url
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        n/a
      </td>
    </tr>
    <tr>
      <td>
        consumer_config_updates
      </td>
      <td>
        <code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>
      </td>
      <td>
        A list of key-value pairs that act as configuration parameters for Kafka consumers. Most of these configurations will not be needed, but if you need to customize your Kafka consumer, you may use this. See a detailed list: https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
      </td>
    </tr>
    <tr>
      <td>
        file_descriptor_path
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The path to the Protocol Buffer File Descriptor Set file. This file is used for schema definition and message serialization.
      </td>
    </tr>
    <tr>
      <td>
        format
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The encoding format for the data stored in Kafka. Valid options are: RAW,STRING,AVRO,JSON,PROTO
      </td>
    </tr>
    <tr>
      <td>
        message_name
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The name of the Protocol Buffer message to be used for schema extraction and data conversion.
      </td>
    </tr>
    <tr>
      <td>
        schema
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The schema in which the data is encoded in the Kafka topic. For AVRO data, this is a schema defined with AVRO schema syntax (https://avro.apache.org/docs/1.10.2/spec.html#schemas). For JSON data, this is a schema defined with JSON-schema syntax (https://json-schema.org/). If a URL to Confluent Schema Registry is provided, then this field is ignored, and the schema is fetched from Confluent Schema Registry.
      </td>
    </tr>
  </table>
</div>

### `KAFKA` Write

<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Configuration</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
    <tr>
      <td>
        <strong>bootstrap_servers</strong>
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this list only impacts the initial hosts used to discover the full set of servers. | Format: host1:port1,host2:port2,...
      </td>
    </tr>
    <tr>
      <td>
        <strong>format</strong>
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The encoding format for the data stored in Kafka. Valid options are: RAW,JSON,AVRO,PROTO
      </td>
    </tr>
    <tr>
      <td>
        <strong>topic</strong>
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        n/a
      </td>
    </tr>
    <tr>
      <td>
        file_descriptor_path
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The path to the Protocol Buffer File Descriptor Set file. This file is used for schema definition and message serialization.
      </td>
    </tr>
    <tr>
      <td>
        message_name
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The name of the Protocol Buffer message to be used for schema extraction and data conversion.
      </td>
    </tr>
    <tr>
      <td>
        producer_config_updates
      </td>
      <td>
        <code>map[<span style="color: green;">str</span>, <span style="color: green;">str</span>]</code>
      </td>
      <td>
        A list of key-value pairs that act as configuration parameters for Kafka producers. Most of these configurations will not be needed, but if you need to customize your Kafka producer, you may use this. See a detailed list: https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
      </td>
    </tr>
    <tr>
      <td>
        schema
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        n/a
      </td>
    </tr>
  </table>
</div>

### `BIGQUERY` Write

<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Configuration</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
    <tr>
      <td>
        <strong>table</strong>
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The bigquery table to write to. Format: [${PROJECT}:]${DATASET}.${TABLE}
      </td>
    </tr>
    <tr>
      <td>
        drop
      </td>
      <td>
        <code>list[<span style="color: green;">str</span>]</code>
      </td>
      <td>
        A list of field names to drop from the input record before writing. Is mutually exclusive with 'keep' and 'only'.
      </td>
    </tr>
    <tr>
      <td>
        keep
      </td>
      <td>
        <code>list[<span style="color: green;">str</span>]</code>
      </td>
      <td>
        A list of field names to keep in the input record. All other fields are dropped before writing. Is mutually exclusive with 'drop' and 'only'.
      </td>
    </tr>
    <tr>
      <td>
        kms_key
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        Use this Cloud KMS key to encrypt your data
      </td>
    </tr>
    <tr>
      <td>
        only
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The name of a single record field that should be written. Is mutually exclusive with 'keep' and 'drop'.
      </td>
    </tr>
    <tr>
      <td>
        triggering_frequency_seconds
      </td>
      <td>
        <code style="color: #f54251">int64</code>
      </td>
      <td>
        Determines how often to 'commit' progress into BigQuery. Default is every 5 seconds.
      </td>
    </tr>
  </table>
</div>

### `BIGQUERY` Read

<div class="table-container-wrapper">
  <table class="table table-bordered">
    <tr>
      <th>Configuration</th>
      <th>Type</th>
      <th>Description</th>
    </tr>
    <tr>
      <td>
        kms_key
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        Use this Cloud KMS key to encrypt your data
      </td>
    </tr>
    <tr>
      <td>
        query
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The SQL query to be executed to read from the BigQuery table.
      </td>
    </tr>
    <tr>
      <td>
        row_restriction
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        Read only rows that match this filter, which must be compatible with Google standard SQL. This is not supported when reading via query.
      </td>
    </tr>
    <tr>
      <td>
        fields
      </td>
      <td>
        <code>list[<span style="color: green;">str</span>]</code>
      </td>
      <td>
        Read only the specified fields (columns) from a BigQuery table. Fields may not be returned in the order specified. If no value is specified, then all fields are returned. Example: "col1, col2, col3"
      </td>
    </tr>
    <tr>
      <td>
        table
      </td>
      <td>
        <code style="color: green">str</code>
      </td>
      <td>
        The fully-qualified name of the BigQuery table to read from. Format: [${PROJECT}:]${DATASET}.${TABLE}
      </td>
    </tr>
  </table>
</div>

