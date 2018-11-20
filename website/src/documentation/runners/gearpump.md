---
layout: section
title: "Apache Gearpump (incubating) Runner"
section_menu: section-menu/runners.html
permalink: /documentation/runners/gearpump/
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
# Using the Apache Gearpump Runner

The Apache Gearpump Runner can be used to execute Beam pipelines using [Apache Gearpump (incubating)](https://gearpump.apache.org).
When you are running your pipeline with Gearpump Runner you just need to create a jar file containing your job and then it can be executed on a regular Gearpump distributed cluster, or a local cluster which is useful for development and debugging of your pipeline.

The Gearpump Runner and Gearpump are suitable for large scale, continuous jobs, and provide:

* High throughput and low latency stream processing
* Comprehensive Dashboard for application monitoring
* Fault-tolerance with exactly-once processing guarantees
* Application hot re-deployment

The [Beam Capability Matrix]({{ site.baseurl }}/documentation/runners/capability-matrix/) documents the currently supported capabilities of the Gearpump Runner.

## Writing Beam Pipeline with Gearpump Runner
To use the Gearpump Runner in a distributed mode, you have to setup a Gearpump cluster first by following the Gearpump [setup quickstart](http://gearpump.apache.org/releases/latest/deployment/deployment-standalone/index.html).

Suppose you are writing a Beam pipeline, you can add a dependency on the latest version of the Gearpump runner by adding to your pom.xml to enable Gearpump runner.
And your Beam application should also pack Beam SDK explicitly and here is a snippet of example pom.xml:
```java
<dependencies>
  <dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-runners-gearpump</artifactId>
    <version>{{ site.release_latest }}</version>
  </dependency>

  <dependency>
    <groupId>org.apache.gearpump</groupId>
    <artifactId>gearpump-streaming_2.11</artifactId>
    <version>${gearpump.version}</version>
    <scope>provided</scope>
  </dependency>

  <dependency>
    <groupId>org.apache.gearpump</groupId>
    <artifactId>gearpump-core_2.11</artifactId>
    <version>${gearpump.version}</version>
    <scope>provided</scope>
  </dependency>

  <dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-core</artifactId>
    <version>{{ site.release_latest }}</version>
  </dependency>
</dependencies>

<build>
  <plugins>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-shade-plugin</artifactId>
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
          </configuration>
        </execution>
      </executions>
    </plugin>
  </plugins>
</build>
```

After running <code>mvn package</code>, run <code>ls target</code> and you should see your application jar like:
```
{your_application}-{version}-shaded.jar
```

## Executing the pipeline on a Gearpump cluster
To run against a Gearpump cluster simply run:
```
gear app -jar /path/to/{your_application}-{version}-shaded.jar com.beam.examples.BeamPipeline --runner=GearpumpRunner ...
```

## Monitoring your application
You can monitor a running Gearpump application using Gearpump's Dashboard. Please follow the Gearpump [Start UI](http://gearpump.apache.org/releases/latest/deployment/deployment-standalone/index.html#start-ui) to start the dashboard.

## Pipeline options for the Gearpump Runner

When executing your pipeline with the Gearpump Runner, you should consider the following pipeline options.

<table class="table table-bordered">
<tr>
  <th>Field</th>
  <th>Description</th>
  <th>Default Value</th>
</tr>
<tr>
  <td><code>runner</code></td>
  <td>The pipeline runner to use. This option allows you to determine the pipeline runner at runtime.</td>
  <td>Set to <code>GearpumpRunner</code> to run using Gearpump.</td>
</tr>
<tr>
  <td><code>parallelism</code></td>
  <td>The parallelism for Gearpump processor.</td>
  <td><code>1</code></td>
</tr>
<tr>
  <td><code>applicationName</code></td>
  <td>The application name for Gearpump runner.</td>
  <td><code>beam_gearpump_app</code></td>
</tr>
</table>
