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

# CdapIO
CdapIO provides I/O transforms for [CDAP](https://cdap.io/) plugins.

## What is CDAP?

[CDAP](https://cdap.io/) is an application platform for building and managing data applications in hybrid and multi-cloud environments.
It enables developers, business analysts, and data scientists to use a visual rapid development environment and utilize common patterns,
data, and application abstractions to accelerate the development of data applications, addressing a broader range of real-time and batch use cases.

[CDAP plugins](https://github.com/data-integrations) types:
- Batch source
- Batch sink
- Streaming source

To learn more about CDAP plugins please see [io.cdap.cdap.api.annotation.Plugin](https://javadoc.io/static/io.cdap.cdap/cdap-api/6.7.2/io/cdap/cdap/api/annotation/Plugin.html) and [Data Integrations](https://github.com/data-integrations) plugins repository.

## CDAP Batch plugins support in CDAP IO

CdapIO supports CDAP Batch plugins based on Hadoop [InputFormat](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/InputFormat.html) and [OutputFormat](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/OutputFormat.html).
CDAP batch plugins support is implemented using [HadoopFormatIO](https://beam.apache.org/documentation/io/built-in/hadoop/).

CdapIO currently supports the following CDAP Batch plugins by referencing `CDAP plugin` class:
* [Hubspot Batch Source](https://github.com/data-integrations/hubspot/blob/develop/src/main/java/io/cdap/plugin/hubspot/source/batch/HubspotBatchSource.java)
* [Hubspot Batch Sink](https://github.com/data-integrations/hubspot/blob/develop/src/main/java/io/cdap/plugin/hubspot/sink/batch/HubspotBatchSink.java)
* [Salesforce Batch Source](https://github.com/data-integrations/salesforce/blob/develop/src/main/java/io/cdap/plugin/salesforce/plugin/source/batch/SalesforceBatchSource.java)
* [Salesforce Batch Sink](https://github.com/data-integrations/salesforce/blob/develop/src/main/java/io/cdap/plugin/salesforce/plugin/sink/batch/SalesforceBatchSink.java)
* [ServiceNow Batch Source](https://github.com/data-integrations/servicenow-plugins/blob/develop/src/main/java/io/cdap/plugin/servicenow/source/ServiceNowSource.java)
* [Zendesk Batch Source](https://github.com/data-integrations/zendesk/blob/develop/src/main/java/io/cdap/plugin/zendesk/source/batch/ZendeskBatchSource.java)

It means that all these plugins can be used like this:
``CdapIO.withCdapPluginClass(HubspotBatchSource.class)``

### Requirements for Cdap Batch plugins

CDAP Batch plugin should be based on `HadoopFormat` implementation.

### How to add support for a new CDAP Batch plugin

To add CdapIO support for a new CDAP Batch [Plugin](src/main/java/org/apache/beam/sdk/io/cdap/Plugin.java) perform the following steps:
1. Find CDAP plugin artifacts in the Maven Central repository. *Example:* [Hubspot plugin Maven repository](https://mvnrepository.com/artifact/io.cdap/hubspot-plugins/1.0.0). *Note:* To add a custom CDAP plugin, please follow [Sonatype publishing guidelines](https://central.sonatype.org/publish/).
2. Add the CDAP plugin Maven dependency to the `build.gradle` file. *Example:* ``implementation "io.cdap:hubspot-plugins:1.0.0"``.
3. Here are two ways of using CDAP batch plugin with CdapIO:
   1. Using `Plugin.createBatch()` method. Pass Cdap Plugin class and correct `InputFormat` (or `OutputFormat`) and `InputFormatProvider` (or `OutputFormatProvider`) classes to CdapIO. *Example:*
   ```
   CdapIO.withCdapPlugin(
      Plugin.createBatch(
      EmployeeBatchSource.class,
      EmployeeInputFormat.class,
      EmployeeInputFormatProvider.class));
   ```
   2. Using `MappingUtils`.
      1. Navigate to [MappingUtils](src/main/java/org/apache/beam/sdk/io/cdap/MappingUtils.java) class.
      2. Modify `getPluginClassByName()` method:
      3. Add the code for mapping Cdap Plugin class name and `Input/Output Format` and `FormatProvider` classes.
      *Example:*
      ```
      if (pluginClass.equals(EmployeeBatchSource.class)){
         return Plugin.createBatch(pluginClass,
                       EmployeeInputFormat.class,
                       EmployeeInputFormatProvider.class);
      }
      ```
      4. After these steps you will be able to use Cdap Plugin by class name like this: ``CdapIO.withCdapPluginClass(EmployeeBatchSource.class)``

To learn more, please check out [complete examples](https://github.com/apache/beam/tree/master/examples/java/cdap/src/main/java/org/apache/beam/examples/complete/cdap).

## CDAP Streaming plugins support in CDAP IO

CdapIO supports CDAP Streaming plugins based on [Apache Spark Receiver](https://spark.apache.org/docs/2.4.0/streaming-custom-receivers.html).
CDAP streaming plugins support is implemented using [SparkReceiverIO](https://github.com/apache/beam/tree/master/sdks/java/io/sparkreceiver).

### Requirements for Cdap Streaming plugins

1. CDAP Streaming plugin should be based on `Spark Receiver`.
2. CDAP Streaming plugin should support work with offsets.
   1. Corresponding Spark Receiver should implement [HasOffset](https://github.com/apache/beam/blob/master/sdks/java/io/sparkreceiver/src/main/java/org/apache/beam/sdk/io/sparkreceiver/HasOffset.java) interface.
   2. Records should have the numeric field that represents record offset. *Example:* `RecordId` field for Salesforce and `vid` field for Hubspot plugins.
   For more details please see [GetOffsetUtils](https://github.com/apache/beam/tree/master/examples/java/cdap/src/main/java/org/apache/beam/examples/complete/cdap/utils/GetOffsetUtils.java) class from examples.

### How to add support for a new CDAP Streaming plugin

To add CdapIO support for a new CDAP Streaming SparkReceiver [Plugin](src/main/java/org/apache/beam/sdk/io/cdap/Plugin.java), perform the following steps:
1. Find CDAP plugin artifacts in the Maven Central repository. *Example:* [Hubspot plugin Maven repository](https://mvnrepository.com/artifact/io.cdap/hubspot-plugins/1.0.0). *Note:* To add a custom CDAP plugin, please follow [Sonatype publishing guidelines](https://central.sonatype.org/publish/).
2. Add CDAP plugin Maven dependency to the `build.gradle` file. *Example:* ``implementation "io.cdap:hubspot-plugins:1.0.0"``.
3. Implement function that will define how to get `Long offset` from the record of the Cdap Plugin.
*Example:* see [GetOffsetUtils](https://github.com/apache/beam/tree/master/examples/java/cdap/src/main/java/org/apache/beam/examples/complete/cdap/utils/GetOffsetUtils.java) class from examples.
4. Here are two ways of using Cdap streaming Plugin with CdapIO:
    1. Using `Plugin.createStreaming()` method. Pass Cdap Plugin class, correct `getOffsetFn` (from step 3) and Spark `Receiver` class to CdapIO. *Example:*
   ```
   CdapIO.withCdapPlugin(
      Plugin.createStreaming(
      HubspotStreamingSource.class,
      offsetFnForHubspot,
      HubspotReceiver.class)));
   ```
    2. Using `MappingUtils`.
        1. Navigate to [MappingUtils](src/main/java/org/apache/beam/sdk/io/cdap/MappingUtils.java) class.
        2. Modify `getPluginClassByName()` method:
        3. Add the code for mapping Cdap Plugin class name, `getOffsetFn` function and Spark `Receiver` class.
           *Example:*
       ```
       if (pluginClass.equals(HubspotStreamingSource.class)){
          return Plugin.createStreaming(pluginClass,
                        getOffsetFnForHubpot(),
                        HubspotReceiverClass.class);
       }
       ```
        4. After these steps you will be able to use Cdap Plugin by class name like this: ``CdapIO.withCdapPluginClass(HubspotStreamingSource.class)``

To learn more, please check out [complete examples](https://github.com/apache/beam/tree/master/examples/java/cdap).

## Dependencies

To use CdapIO please add a dependency on `beam-sdks-java-io-cdap`.

```maven
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-cdap</artifactId>
    <version>...</version>
</dependency>
```

## Documentation

The documentation and usage examples are maintained in JavaDoc for [CdapIO.java](src/main/java/org/apache/beam/sdk/io/cdap/CdapIO.java).
