---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

name: runners
description: Guides understanding and working with Apache Beam runners (Direct, Dataflow, Flink, Spark, etc.). Use when configuring pipelines for different execution environments or debugging runner-specific issues.
---

# Apache Beam Runners

## Overview
Runners execute Beam pipelines on distributed processing backends. Each runner translates the portable Beam model to its native execution engine.

## Available Runners

| Runner | Location | Description |
|--------|----------|-------------|
| Direct | `runners/direct-java/` | Local execution for testing |
| Prism | `runners/prism/` | Portable local runner |
| Dataflow | `runners/google-cloud-dataflow-java/` | Google Cloud Dataflow |
| Flink | `runners/flink/` | Apache Flink |
| Spark | `runners/spark/` | Apache Spark |
| Samza | `runners/samza/` | Apache Samza |
| Jet | `runners/jet/` | Hazelcast Jet |
| Twister2 | `runners/twister2/` | Twister2 |

## Direct Runner
For local development and testing.

### Java
```java
PipelineOptions options = PipelineOptionsFactory.create();
options.setRunner(DirectRunner.class);
Pipeline p = Pipeline.create(options);
```

### Python
```python
options = PipelineOptions()
options.view_as(StandardOptions).runner = 'DirectRunner'
p = beam.Pipeline(options=options)
```

### Command Line
```bash
--runner=DirectRunner
```

## Dataflow Runner

### Prerequisites
- GCP project with Dataflow API enabled
- Service account with Dataflow Admin role
- GCS bucket for staging

### Java Usage
```java
DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
options.setRunner(DataflowRunner.class);
options.setProject("my-project");
options.setRegion("us-central1");
options.setTempLocation("gs://my-bucket/temp");
```

### Python Usage
```python
options = PipelineOptions([
    '--runner=DataflowRunner',
    '--project=my-project',
    '--region=us-central1',
    '--temp_location=gs://my-bucket/temp'
])
```

### Runner v2
```bash
--experiments=use_runner_v2
```

### Custom SDK Container
```bash
--sdkContainerImage=gcr.io/project/beam_java11_sdk:custom
```

## Flink Runner

### Embedded Mode
```java
FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
options.setRunner(FlinkRunner.class);
options.setFlinkMaster("[local]");
```

### Cluster Mode
```java
options.setFlinkMaster("host:port");
```

### Portable Mode (Python)
```python
options = PipelineOptions([
    '--runner=FlinkRunner',
    '--flink_master=host:port',
    '--environment_type=LOOPBACK'  # or DOCKER, EXTERNAL
])
```

## Spark Runner

### Java
```java
SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
options.setRunner(SparkRunner.class);
options.setSparkMaster("local[*]");  # or spark://host:port
```

### Python (Portable)
```python
options = PipelineOptions([
    '--runner=SparkRunner',
    '--spark_master_url=local[*]'
])
```

## Testing with Runners

### ValidatesRunner Tests
Tests that validate runner correctness:
```bash
# Direct Runner
./gradlew :runners:direct-java:validatesRunner

# Flink Runner
./gradlew :runners:flink:1.18:validatesRunner

# Spark Runner
./gradlew :runners:spark:3:validatesRunner

# Dataflow Runner
./gradlew :runners:google-cloud-dataflow-java:validatesRunner
```

### TestPipeline with Runners
```java
@Rule public TestPipeline pipeline = TestPipeline.create();

// Set runner via system property
-DbeamTestPipelineOptions='["--runner=TestDataflowRunner"]'
```

## Portable Runners

### Concept
- SDK-independent execution via Fn API
- SDK runs in container, communicates via gRPC

### Environment Types
- `DOCKER` - SDK in Docker container
- `LOOPBACK` - SDK in same process (testing)
- `EXTERNAL` - SDK at specified address
- `PROCESS` - SDK in subprocess

### Job Server
Start Flink job server:
```bash
./gradlew :runners:flink:1.18:job-server:runShadow
```

Start Spark job server:
```bash
./gradlew :runners:spark:3:job-server:runShadow
```

## Runner-Specific Options

### Dataflow
| Option | Description |
|--------|-------------|
| `--project` | GCP project |
| `--region` | GCP region |
| `--tempLocation` | GCS temp location |
| `--stagingLocation` | GCS staging |
| `--numWorkers` | Initial workers |
| `--maxNumWorkers` | Max workers |
| `--workerMachineType` | VM type |

### Flink
| Option | Description |
|--------|-------------|
| `--flinkMaster` | Flink master address |
| `--parallelism` | Default parallelism |
| `--checkpointingInterval` | Checkpoint interval |

### Spark
| Option | Description |
|--------|-------------|
| `--sparkMaster` | Spark master URL |
| `--sparkConf` | Additional Spark config |

## Building Runner Artifacts

### Dataflow Worker Jar
```bash
./gradlew :runners:google-cloud-dataflow-java:worker:shadowJar
```

### Flink Job Server
```bash
./gradlew :runners:flink:1.18:job-server:shadowJar
```

### Spark Job Server
```bash
./gradlew :runners:spark:3:job-server:shadowJar
```

## Debugging

### Direct Runner
- Enable logging: `-Dorg.slf4j.simpleLogger.defaultLogLevel=debug`
- Use `--targetParallelism=1` for deterministic execution

### Dataflow
- Check Dataflow UI: console.cloud.google.com/dataflow
- Use `--experiments=upload_graph` for graph debugging
- Worker logs in Cloud Logging

### Portable Runners
- Enable debug logging on job server
- Check SDK harness logs in worker containers
