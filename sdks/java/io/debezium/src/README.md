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

# DebeziumIO
## Connect your Debezium Databases to Apache Beam easily.

### What is DebeziumIO?
DebeziumIO is an Apache Beam connector that lets users connect their Events-Driven Databases on [Debezium](https://debezium.io) to [Apache Beam](https://beam.apache.org/) without the need to set up a [Kafka](https://kafka.apache.org/) instance.

### Getting Started

DebeziumIO uses [Debezium Connectors v1.3](https://debezium.io/documentation/reference/1.3/connectors/) to connect to Apache Beam. All you need to do is choose the Debezium Connector that suits your Debezium setup and pick a [Serializable Function](https://beam.apache.org/releases/javadoc/2.23.0/org/apache/beam/sdk/transforms/SerializableFunction.html), then you will be able to connect to Apache Beam and start building your own Pipelines.

These connectors have been successfully tested and are known to work fine:
*  MySQL Connector
*  PostgreSQL Connector
*  SQLServer Connector
*  DB2 Connector

Other connectors might also work.


Setting up a connector and running a Pipeline should be as simple as:
```
Pipeline p = Pipeline.create();                   // Create a Pipeline
        p.apply(DebeziumIO.<String>read()
                .withConnectorConfiguration(...)  // Debezium Connector setup
                .withFormatFunction(...)          // Serializable Function to use
        ).setCoder(StringUtf8Coder.of());
p.run().waitUntilFinish();                        // Run your pipeline!
```

### Setting up a Debezium Connector

DebeziumIO comes with a handy ConnectorConfiguration builder, which lets you provide all the configuration needed to access your Debezium Database.

A basic configuration such as **username**, **password**, **port number**, and **host name** must be specified along with the **Debezium Connector class** you will use by using these methods:

|Method|Param|Description|
|-|-|-|
|`.withConnectorClass(connectorClass)`|_Class_|Debezium Connector|
|`.withUsername(username)`|_String_|Database Username|
|`.withPassword(password)`|_String_|Database Password|
|`.withHostName(hostname)`|_String_|Database Hostname|
|`.withPort(portNumber)`|_String_|Database Port number|

You can also add more configuration, such as Connector-specific Properties with the `_withConnectionProperty_` method:

|Method|Params|Description|
|-|-|-|
|`.withConnectionProperty(propName, propValue)`|_String_, _String_|Adds a custom property to the connector.|
> **Note:** For more information on custom properties, see your [Debezium Connector](https://debezium.io/documentation/reference/1.3/connectors/) specific documentation.

Example of a MySQL Debezium Connector setup:
```
DebeziumIO.ConnectorConfiguration.create()
        .withUsername("dbUsername")
        .withPassword("dbPassword")
        .withConnectorClass(MySqlConnector.class)
        .withHostName("127.0.0.1")
        .withPort("3306")
        .withConnectionProperty("database.server.id", "serverId")
        .withConnectionProperty("database.server.name", "serverName")
        .withConnectionProperty("database.include.list", "dbName")
        .withConnectionProperty("include.schema.changes", "false")
```

### Setting a Serializable Function

A serializable function is required to depict each `SourceRecord` fetched from the Database.

DebeziumIO comes with a built-in JSON Mapper that you can optionally use to map every `SourceRecord` fetched from the Database to a JSON object. This helps users visualize and access their data in a simple way.

If you want to use this built-in JSON Mapper, you can do it by setting an instance of **SourceRecordJsonMapper** as a Serializable Function to the DebeziumIO:
```
.withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
```
> **Note:** `SourceRecordJsonMapper`comes out of the box, but you may use any Format Function you prefer.

## Quick Example

The following example is how an actual setup would look like using a **MySQL Debezium Connector** and **SourceRecordJsonMapper** as the Serializable Function.
```
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline p = Pipeline.create(options);
p.apply(DebeziumIO.<String>read().
        withConnectorConfiguration(                     // Debezium Connector setup
                DebeziumIO.ConnectorConfiguration.create()
                        .withUsername("debezium")
                        .withPassword("dbz")
                        .withConnectorClass(MySqlConnector.class)
                        .withHostName("127.0.0.1")
                        .withPort("3306")
                        .withConnectionProperty("database.server.id", "184054")
                        .withConnectionProperty("database.server.name", "dbserver1")
                        .withConnectionProperty("database.include.list", "inventory")
                        .withConnectionProperty("include.schema.changes", "false")
        ).withFormatFunction(
                new SourceRecordJson.SourceRecordJsonMapper() // Serializable Function
        )
).setCoder(StringUtf8Coder.of());

p.run().waitUntilFinish();
```

## Shortcut!

If you will be using the built-in **SourceRecordJsonMapper** as your Serializable Function for all your pipelines, you should use **readAsJson()**.

DebeziumIO comes with a method called `readAsJson`, which automatically sets the `SourceRecordJsonMapper` as the Serializable Function for your pipeline. This way, you would need to setup your connector before running your pipeline, without explicitly setting a Serializable Function.

Example of using **readAsJson**:
```
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline p = Pipeline.create(options);
p.apply(DebeziumIO.<String>read().
        withConnectorConfiguration(                     // Debezium Connector setup
                DebeziumIO.ConnectorConfiguration.create()
                        .withUsername("debezium")
                        .withPassword("dbz")
                        .withConnectorClass(MySqlConnector.class)
                        .withHostName("127.0.0.1")
                        .withPort("3306")
                        .withConnectionProperty("database.server.id", "184054")
                        .withConnectionProperty("database.server.name", "dbserver1")
                        .withConnectionProperty("database.include.list", "inventory")
                        .withConnectionProperty("include.schema.changes", "false"));

p.run().waitUntilFinish();
```

## Under the hood

### KafkaSourceConsumerFn and Restrictions

KafkaSourceConsumerFn (KSC onwards) is a [DoFn](https://beam.apache.org/releases/javadoc/2.3.0/org/apache/beam/sdk/transforms/DoFn.html) in charge of the Database replication and CDC.

There are two ways of initializing KSC:
*  Restricted by number of records
*  Restricted by amount of time (minutes)

By default, DebeziumIO initializes it with the former, though user may choose the latter by setting the amount of minutes as a parameter:

|Function|Param|Description|
|-|-|-|
|`KafkaSourceConsumerFn(connectorClass, recordMapper, maxRecords)`|_Class, SourceRecordMapper, Int_|Restrict run by number of records (Default).|
|`KafkaSourceConsumerFn(connectorClass, recordMapper, timeToRun)`|_Class, SourceRecordMapper, Long_|Restrict run by amount of time (in minutes).|

### Requirements and Supported versions

-  JDK v8
-  Debezium Connectors v1.3
-  Apache Beam 2.25

## Running Unit Tests

You can run Integration Tests using **gradlew**.

Example of running the MySQL Connector Integration Test:
```
./gradlew integrationTest -p sdks/java/io/debezium/ --tests org.apache.beam.io.debezium.DebeziumIOMySqlConnectorIT -DintegrationTestRunner=direct
```