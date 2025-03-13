---
title: "Cdap IO"
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

# Cdap IO

A `CdapIO` is a transform for reading data from source or writing data to sink CDAP plugin.

## Batch plugins support

`CdapIO` currently supports the following CDAP Batch plugins by referencing `CDAP plugin` class name:
- [Hubspot Batch Source](https://github.com/data-integrations/hubspot/blob/develop/src/main/java/io/cdap/plugin/hubspot/source/batch/HubspotBatchSource.java)
- [Hubspot Batch Sink](https://github.com/data-integrations/hubspot/blob/develop/src/main/java/io/cdap/plugin/hubspot/sink/batch/HubspotBatchSink.java)
- [Salesforce Batch Source](https://github.com/data-integrations/salesforce/blob/develop/src/main/java/io/cdap/plugin/salesforce/plugin/source/batch/SalesforceBatchSource.java)
- [Salesforce Batch Sink](https://github.com/data-integrations/salesforce/blob/develop/src/main/java/io/cdap/plugin/salesforce/plugin/sink/batch/SalesforceBatchSink.java)
- [ServiceNow Batch Source](https://github.com/data-integrations/servicenow-plugins/blob/develop/src/main/java/io/cdap/plugin/servicenow/source/ServiceNowSource.java)
- [Zendesk Batch Source](https://github.com/data-integrations/zendesk/blob/develop/src/main/java/io/cdap/plugin/zendesk/source/batch/ZendeskBatchSource.java)

Also, any other CDAP Batch plugin based on Hadoop's `InputFormat` or `OutputFormat` can be used. They can be easily added to the list of supported by class name plugins, for more details please see [CdapIO readme](https://github.com/apache/beam/blob/master/sdks/java/io/cdap/README.md).

## Streaming plugins support

`CdapIO` currently supports CDAP Streaming plugins based on [Apache Spark Receiver](https://spark.apache.org/docs/2.4.0/streaming-custom-receivers.html).

Requirements for CDAP Streaming plugins:
- CDAP Streaming plugin should be based on `Spark Receiver` (Spark 2.4).
- CDAP Streaming plugin should support work with offsets.
- Corresponding Spark Receiver should implement [HasOffset](https://github.com/apache/beam/blob/master/sdks/java/io/sparkreceiver/2/src/main/java/org/apache/beam/sdk/io/sparkreceiver/HasOffset.java) interface.
- Records should have the numeric field that represents record offset.

## Batch reading using CdapIO

In order to read from CDAP plugin you will need to pass:
- `Key` and `Value` classes. You will need to check if these classes have a Beam Coder available.
- `PluginConfig` object with parameters for certain CDAP plugin.

You can easily build `PluginConfig` object using `ConfigWrapper` class by specifying:

- Class of the needed `PluginConfig`.
- `Map<String, Object>` parameters map for corresponding CDAP plugin.

For example:

{{< highlight java >}}
Map<String, Object> myPluginConfigParams = new HashMap<>();
// Read plugin parameters (e.g. from PipelineOptions) and put them into 'myPluginConfigParams' map.
myPluginConfigParams.put(MyPluginConstants.USERNAME_PARAMETER_NAME, pipelineOptions.getUsername());
// ...
MyPluginConfig pluginConfig =
  new ConfigWrapper<>(MyPluginConfig.class).withParams(myPluginConfigParams).build();
{{< /highlight >}}

### Read data by plugin class name

Some CDAP plugins are already supported and can be used just by plugin class name.

For example:

{{< highlight java >}}
CdapIO.Read<NullWritable, JsonElement> readTransform =
  CdapIO.<NullWritable, JsonElement>read()
    .withCdapPluginClass(HubspotBatchSource.class)
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(JsonElement.class);
p.apply("read", readTransform);
{{< /highlight >}}

### Read data with building Batch Plugin

If CDAP plugin is not supported by plugin class name, you can easily build `Plugin` object by passing the following parameters:

- Class of CDAP Batch plugin.
- The `InputFormat` class used to connect to your CDAP plugin of choice.
- The `InputFormatProvider` class used to provide `InputFormat`.

Then you will be able to pass this `Plugin` object to `CdapIO`.

For example:

{{< highlight java >}}
CdapIO.Read<String, String> readTransform =
  CdapIO.<String, String>read()
    .withCdapPlugin(
      Plugin.createBatch(
        MyCdapPlugin.class,
        MyInputFormat.class,
        MyInputFormatProvider.class))
    .withPluginConfig(pluginConfig)
    .withKeyClass(String.class)
    .withValueClass(String.class);
p.apply("read", readTransform);
{{< /highlight >}}

### Examples for specific CDAP plugins

#### CDAP Hubspot Batch Source plugin

{{< highlight java >}}
SourceHubspotConfig pluginConfig =
  new ConfigWrapper<>(SourceHubspotConfig.class).withParams(pluginConfigParams).build();
CdapIO<NullWritable, JsonElement> readTransform =
  CdapIO.<NullWritable, JsonElement>read()
    .withCdapPluginClass(HubspotBatchSource.class)
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(JsonElement.class);
p.apply("readFromHubspotPlugin", readTransform);
{{< /highlight >}}

#### CDAP Salesforce Batch Source plugin

{{< highlight java >}}
SalesforceSourceConfig pluginConfig =
  new ConfigWrapper<>(SalesforceSourceConfig.class).withParams(pluginConfigParams).build();
CdapIO<Schema, LinkedHashMap> readTransform =
  CdapIO.<Schema, LinkedHashMap>read()
    .withCdapPluginClass(SalesforceBatchSource.class)
    .withPluginConfig(pluginConfig)
    .withKeyClass(Schema.class)
    .withValueClass(LinkedHashMap.class);
p.apply("readFromSalesforcePlugin", readTransform);
{{< /highlight >}}

#### CDAP ServiceNow Batch Source plugin

{{< highlight java >}}
ServiceNowSourceConfig pluginConfig =
  new ConfigWrapper<>(ServiceNowSourceConfig.class).withParams(pluginConfigParams).build();
CdapIO<NullWritable, StructuredRecord> readTransform =
  CdapIO.<NullWritable, StructuredRecord>read()
    .withCdapPluginClass(ServiceNowSource.class)
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(StructuredRecord.class);
p.apply("readFromServiceNowPlugin", readTransform);
{{< /highlight >}}

#### CDAP Zendesk Batch Source plugin

{{< highlight java >}}
ZendeskBatchSourceConfig pluginConfig =
  new ConfigWrapper<>(ZendeskBatchSourceConfig.class).withParams(pluginConfigParams).build();
CdapIO<NullWritable, StructuredRecord> readTransform =
  CdapIO.<NullWritable, StructuredRecord>read()
    .withCdapPluginClass(ZendeskBatchSource.class)
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(StructuredRecord.class);
p.apply("readFromZendeskPlugin", readTransform);
{{< /highlight >}}

To learn more please check out [complete examples](https://github.com/apache/beam/tree/master/examples/java/cdap).

## Batch writing using CdapIO

In order to write to CDAP plugin you will need to pass:
- `Key` and `Value` classes. You will need to check if these classes have a Beam Coder available.
- `locksDirPath`, which is locks directory path where locks will be stored. This parameter is needed for Hadoop External Synchronization (mechanism for acquiring locks related to the write job).
- `PluginConfig` object with parameters for certain CDAP plugin.

You can easily build `PluginConfig` object using `ConfigWrapper` class by specifying:

- Class of the needed `PluginConfig`.
- `Map<String, Object>` parameters map for corresponding CDAP plugin.

For example:

{{< highlight java >}}
MyPluginConfig pluginConfig =
  new ConfigWrapper<>(MyPluginConfig.class).withParams(pluginConfigParams).build();
{{< /highlight >}}

### Write data by plugin class name

Some CDAP plugins are already supported and can be used just by plugin class name.

For example:

{{< highlight java >}}
CdapIO.Write<NullWritable, String> readTransform =
  CdapIO.<NullWritable, String>write()
    .withCdapPluginClass(HubspotBatchSink.class)
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(String.class)
    .withLocksDirPath(locksDirPath);
p.apply("write", writeTransform);
{{< /highlight >}}

### Write data with building Batch Plugin

If CDAP plugin is not supported by plugin class name, you can easily build `Plugin` object by passing the following parameters:

- Class of CDAP plugin.
- The `OutputFormat` class used to connect to your CDAP plugin of choice.
- The `OutputFormatProvider` class used to provide `OutputFormat`.

Then you will be able to pass this `Plugin` object to `CdapIO`.

For example:

{{< highlight java >}}
CdapIO.Write<String, String> writeTransform =
  CdapIO.<String, String>write()
    .withCdapPlugin(
      Plugin.createBatch(
        MyCdapPlugin.class,
        MyOutputFormat.class,
        MyOutputFormatProvider.class))
    .withPluginConfig(pluginConfig)
    .withKeyClass(String.class)
    .withValueClass(String.class)
    .withLocksDirPath(locksDirPath);
p.apply("write", writeTransform);
{{< /highlight >}}

### Examples for specific CDAP plugins

#### CDAP Hubspot Batch Sink plugin

{{< highlight java >}}
SinkHubspotConfig pluginConfig =
  new ConfigWrapper<>(SinkHubspotConfig.class).withParams(pluginConfigParams).build();
CdapIO<NullWritable, String> writeTransform =
  CdapIO.<NullWritable, String>write()
    .withCdapPluginClass(pluginClass)
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(String.class)
    .withLocksDirPath(locksDirPath);
p.apply("writeToHubspotPlugin", writeTransform);
{{< /highlight >}}

#### CDAP Salesforce Batch Sink plugin

{{< highlight java >}}
SalesforceSinkConfig pluginConfig =
  new ConfigWrapper<>(SalesforceSinkConfig.class).withParams(pluginConfigParams).build();
CdapIO<NullWritable, CSVRecord> writeTransform =
  CdapIO.<NullWritable, CSVRecord>write()
    .withCdapPluginClass(pluginClass)
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(CSVRecord.class)
    .withLocksDirPath(locksDirPath);
p.apply("writeToSalesforcePlugin", writeTransform);
{{< /highlight >}}

To learn more please check out [complete examples](https://github.com/apache/beam/tree/master/examples/java/cdap/src/main/java/org/apache/beam/examples/complete/cdap).

## Streaming reading using CdapIO

In order to read from CDAP plugin you will need to pass:
- `Key` and `Value` classes. You will need to check if these classes have a Beam Coder available.
- `PluginConfig` object with parameters for certain CDAP plugin.

You can easily build `PluginConfig` object using `ConfigWrapper` class by specifying:

- Class of the needed `PluginConfig`.
- `Map<String, Object>` parameters map for corresponding CDAP plugin.

For example:

{{< highlight java >}}
MyPluginConfig pluginConfig =
  new ConfigWrapper<>(MyPluginConfig.class).withParams(pluginConfigParams).build();
{{< /highlight >}}

### Read data by plugin class name

Some CDAP plugins are already supported and can be used just by plugin class name.

For example:

{{< highlight java >}}
CdapIO.Read<String, String> readTransform =
  CdapIO.<String, String>read()
    .withCdapPluginClass(MyStreamingPlugin.class)
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(String.class);
p.apply("read", readTransform);
{{< /highlight >}}

### Read data with building Streaming Plugin

If CDAP plugin is not supported by plugin class name, you can easily build `Plugin` object by passing the following parameters:

- Class of CDAP Streaming plugin.
- `getOffsetFn`, which is `SerializableFunction` that defines how to get `Long` record offset from a record.
- `receiverClass`, which is Spark (v 2.4) `Receiver` class associated with CDAP plugin.
- (Optionally) `getReceiverArgsFromConfigFn`, which is `SerializableFunction` that defines how to get constructor arguments for Spark `Receiver` using `PluginConfig` object.

Then you will be able to pass this `Plugin` object to `CdapIO`.

For example:

{{< highlight java >}}
CdapIO.Read<String, String> readTransform =
  CdapIO.<String, String>read()
    .withCdapPlugin(
      Plugin.createStreaming(
        MyStreamingPlugin.class,
        myGetOffsetFn,
        MyReceiver.class,
        myGetReceiverArgsFromConfigFn))
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(String.class);
p.apply("read", readTransform);
{{< /highlight >}}

### Read data with optional parameters

Optionally you can pass the following optional parameters:

- `pullFrequencySec`, which is delay in seconds between polling for new records updates.
- `startOffset`, which is inclusive start offset from which the reading should be started.

For example:

{{< highlight java >}}
CdapIO.Read<String, String> readTransform =
  CdapIO.<String, String>read()
    .withCdapPluginClass(MyStreamingPlugin.class)
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(String.class)
    .withPullFrequencySec(1L)
    .withStartOffset(1L);
p.apply("read", readTransform);
{{< /highlight >}}

### Examples for specific CDAP plugins

#### CDAP Hubspot Streaming Source plugin

{{< highlight java >}}
HubspotStreamingSourceConfig pluginConfig =
  new ConfigWrapper<>(HubspotStreamingSourceConfig.class)
    .withParams(pluginConfigParams).build();
CdapIO.Read<NullWritable, String> readTransform =
  CdapIO.<NullWritable, String>read()
    .withCdapPlugin(
      Plugin.createStreaming(
        HubspotStreamingSource.class,
        GetOffsetUtils.getOffsetFnForHubspot(),
        HubspotReceiver.class))
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(String.class);
p.apply("readFromHubspotPlugin", readTransform);
{{< /highlight >}}

#### CDAP Salesforce Streaming Source plugin

{{< highlight java >}}
SalesforceStreamingSourceConfig pluginConfig =
  new ConfigWrapper<>(SalesforceStreamingSourceConfig.class)
    .withParams(pluginConfigParams).build();
CdapIO.Read<NullWritable, String> readTransform =
  CdapIO.<NullWritable, String>read()
    .withCdapPlugin(
      Plugin.createStreaming(
        SalesforceStreamingSource.class,
        GetOffsetUtils.getOffsetFnForSalesforce(),
        SalesforceReceiver.class,
        config -> {
          SalesforceStreamingSourceConfig salesforceConfig =
            SalesforceStreamingSourceConfig) config;
          return new Object[] {
            salesforceConfig.getAuthenticatorCredentials(),
            salesforceConfig.getPushTopicName()
          };
        }))
    .withPluginConfig(pluginConfig)
    .withKeyClass(NullWritable.class)
    .withValueClass(String.class);
p.apply("readFromSalesforcePlugin", readTransform);
{{< /highlight >}}

To learn more please check out [complete examples](https://github.com/apache/beam/tree/master/examples/java/cdap/src/main/java/org/apache/beam/examples/complete/cdap).
