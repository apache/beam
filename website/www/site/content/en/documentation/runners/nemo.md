---
type: runners
title: "Apache Nemo Runner"
aliases: /learn/runners/nemo/
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
# Using the Apache Nemo Runner

The Apache Nemo Runner can be used to execute Beam pipelines using [Apache Nemo](https://nemo.apache.org).
The Nemo Runner can optimize Beam pipelines with the Nemo compiler through various optimization passes
and execute them in a distributed fashion using the Nemo runtime. You can also deploy a self-contained application
for local mode or run using resource managers like YARN or Mesos.

The Nemo Runner executes Beam pipelines on top of Apache Nemo, providing:

* Batch and streaming pipelines
* Fault-tolerance
* Integration with YARN and other components of the Apache Hadoop ecosystem
* Support for the various optimizations provided by the Nemo optimizer

The [Beam Capability Matrix](/documentation/runners/capability-matrix/) documents the
supported capabilities of the Nemo Runner.

## Nemo Runner prerequisites and setup

The Nemo Runner can be used simply by adding a dependency on a version of the Nemo runner newer than `0.1`
to your pom.xml as follows:

```
<dependency>
    <groupId>org.apache.nemo</groupId>
    <artifactId>nemo-compiler-frontend-beam</artifactId>
    <version>${nemo.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>${hadoop.version}</version>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </exclusion>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

## Deploying Nemo with your Application

A self-contained application might be easier to manage and allows you to fully use the functionality that Nemo provides.
Simply add the dependency shown above and shade the application JAR using the Maven Shade plugin:

```
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
        <transformers>
          <transformer
            implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
        </transformers>
      </configuration>
    </execution>
  </executions>
</plugin>
```

After running `mvn package`, run `ls target` and you should see the following output (in this example, your artifactId is `beam-examples`
and the version is `1.0.0`):

```
beam-examples-1.0.0-shaded.jar
```

With this shaded jar, you can use the `bin/run_beam.sh` shell script as follows:

```
## MapReduce example
./bin/run_beam.sh \
    -job_id mr_default \
    -user_main org.apache.nemo.examples.beam.WordCount \
    -user_args "`pwd`/examples/resources/test_input_wordcount `pwd`/examples/resources/test_output_wordcount"
```

To use Nemo using YARN, set the `-deploy_mode` flag on Nemo to `yarn`. 

More instructions can be seen on the README of the [Apache Nemo GitHub](https://github.com/apache/incubator-nemo).

## Pipeline Options for the Nemo Runner

When executing your pipeline with the Nemo Runner, you should consider the following pipeline options:

| Field       | Description           | Default Value  |
| ------------- |---------------| -----:|
| `runner`      | The pipeline runner to use. This option allows you to determine the pipeline runner at runtime. | Set to `NemoRunner` to run using Nemo. |
| `maxBundleSize`      | The maximum number of elements in a bundle. |   1000 |
| `maxBundleTimeMillis` | The maximum time to wait before finalizing a bundle (in milliseconds). |   1000 |

More options are to be added to the list, to fully support the various options that Nemo supports.

## Additional Information and Caveats

### Using the Run_beam.sh script

When submitting a Nemo application to the cluster, it is common to use the `bin/run_beam.sh` script that is
provided within the Nemo installation. The script also provides a richer set of options that you can pass on to
configure various actions of Nemo. Please refer to the
[Apache Nemo GitHub README](https://github.com/apache/incubator-nemo) for more information.

### Monitoring your job

You can monitor a running Nemo job using the Nemo WebUI. The docs are currently being updated, but more
information can be found on the [Apache Nemo GitHub README](https://github.com/apache/incubator-nemo).

### Streaming Execution

Add the options `-scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler`
and `-optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy` to set the Nemo Runner
to streaming mode.
Also, be sure to extend the `capacity` of the resources in the `resources.json`, for example:

```
{
  "type": "Reserved",
  "memory_mb": 2048,
  "capacity": 50000
}
```

Please refer to the [Apache Nemo GitHub README](https://github.com/apache/incubator-nemo) for more information.
