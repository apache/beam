---
title: "Apache HCatalog I/O connector"
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

# HCatalog IO

An `HCatalogIO` is a transform for reading and writing data to an HCatalog managed source.

### Reading using HCatalogIO

To configure an HCatalog source, you must specify a metastore URI and a table name. Other optional parameters are database and filter.

For example:

{{< highlight java >}}
Map<String, String> configProperties = new HashMap<String, String>();
configProperties.put("hive.metastore.uris","thrift://metastore-host:port");
pipeline
  .apply(HCatalogIO.read()
  .withConfigProperties(configProperties)
  .withDatabase("default") //optional, assumes default if none specified
  .withTable("employee")
  .withFilter(filterString)) //optional, may be specified if the table is partitioned
{{< /highlight >}}

{{< highlight py >}}
  # The Beam SDK for Python does not support HCatalogIO.
{{< /highlight >}}

### Writing using HCatalogIO

To configure an `HCatalog` sink, you must specify a metastore URI and a table name. Other
optional parameters are database, partition and batchsize.
The destination table should exist beforehand as the transform will not create a new table if missing.

For example:
{{< highlight java >}}
Map<String, String> configProperties = new HashMap<String, String>();
configProperties.put("hive.metastore.uris","thrift://metastore-host:port");

pipeline
  .apply(...)
  .apply(HCatalogIO.write()
    .withConfigProperties(configProperties)
    .withDatabase("default") //optional, assumes default if none specified
    .withTable("employee")
    .withPartition(partitionValues) //optional, may be specified if the table is partitioned
    .withBatchSize(1024L)) //optional, assumes a default batch size of 1024 if none specified
{{< /highlight >}}

{{< highlight py >}}
  # The Beam SDK for Python does not support HCatalogIO.
{{< /highlight >}}

### Using older versions of HCatalog (1.x)

`HCatalogIO` is built for Apache HCatalog versions 2 and up and will not work out of the box for older versions of HCatalog.
The following illustrates a workaround to work with Hive 1.1.

Include the following Hive 1.2 jars in the über jar you build.
The 1.2 jars provide the necessary methods for Beam while remain compatible with Hive 1.1.

```
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-hcatalog</artifactId>
    <version>${beam.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.hive.hcatalog</groupId>
    <artifactId>hive-hcatalog-core</artifactId>
    <version>1.2</version>
</dependency>
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-metastore</artifactId>
    <version>1.2</version>
</dependency>
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-exec</artifactId>
    <version>1.2</version>
</dependency>
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-common</artifactId>
    <version>1.2</version>
</dependency>
```

Relocate _only_ the following hive packages:

```
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>${maven-shade-plugin.version}</version>
    <configuration>
        <createDependencyReducedPom>false</createDependencyReducedPom>
        <filters>
            <filter>
                <artifact>*:*</artifact>
                <excludes>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                </excludes>
            </filter>
        </filters>
    </configuration>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <shadedArtifactAttached>true</shadedArtifactAttached>
                <shadedClassifierName>shaded</shadedClassifierName>
                <transformers>
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                </transformers>
                <relocations>
                    <!-- Important: Do not relocate org.apache.hadoop.hive -->
                    <relocation>
                        <pattern>org.apache.hadoop.hive.conf</pattern>
                        <shadedPattern>h12.org.apache.hadoop.hive.conf</shadedPattern>
                    </relocation>
                    <relocation>
                        <pattern>org.apache.hadoop.hive.ql</pattern>
                        <shadedPattern>h12.org.apache.hadoop.hive.ql</shadedPattern>
                    </relocation>
                    <relocation>
                        <pattern>org.apache.hadoop.hive.metastore</pattern>
                        <shadedPattern>h12.org.apache.hadoop.hive.metastore</shadedPattern>
                    </relocation>
                </relocations>
            </configuration>
        </execution>
    </executions>
</plugin>
```

This has been testing to read SequenceFile and ORCFile file backed tables running with
Beam 2.4.0 on Spark 2.3 / YARN in a Cloudera CDH 5.12.2 managed environment.
