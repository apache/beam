---
type: runners
title: "FlareDB Runner"
aliases: /learn/runners/flaredb/
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

# Overview

[FlareDB](https://github.com/flare-db/flare-db) is an Apache Beam native streaming database for running Beam pipelines. It's built in Rust, a modern systems programming language, and it uses a streams-tables architecture inspired by the ideas described in the *Streaming Systems* book (Chp 6).

<br/>

The idea is that streams are data in motion, produced by computations (transforms), and a table is the same data at rest, during a windowing or grouping operation. FlareDB persists the PCollections as durable streams/tables on an append-only Apache Paimon table and runs the computations/transforms over the table using Apache DataFusion, a high-performance query engine.

<br/>

As a result, FlareDB is lightweight, takes fewer resources to run, and makes the computational results queryable without the need for an external database.

See the [Beam Capability Matrix](https://docs.flare-db.com/compatibility) for supported Beam features.

> **Note:** FlareDB is an independent and open-source runner for Apache Beam. It is not part of, or maintained by, the Apache Beam project. The source code is available on [GitHub](https://github.com/flare-db/flare-db).

# How to use FlareDB Runner

Install FlareDB CLI to spawn up and manage FlareDB instance and run Beam pipelines.

## 1. Install the FlareDB CLI

If you are on **Linux or macOS**, please run the following command to install the CLI:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/flare-db/flare-db/releases/download/flare-cli-v0.3.2/flare-cli-installer.sh | sh
```

If you are on **Windows** use WSL.

## 2. Initialize FlareDB

After installing the CLI, run:

```bash
flare init
```

This command performs the initial setup by creating the required local directories and downloading the FlareDB binary and Apache Beam worker JAR.

The initialization only needs to be **completed once**. After that, you can use the `flare up` and `flare down` commands to manage the instance.


## 3. Start a FlareDB Instance

Start a local FlareDB instance with:

```bash
flare up
```

Once the instance is running, FlareDB is ready to accept pipeline jobs.


## 4. Configure Your Beam Pipeline

To run an Apache Beam pipeline on FlareDB, add the FlareDB Runner SDK as a dependency to your Beam project. The runner SDK submits the pipeline to the FlareDB instance as a Job.

{{< language-switcher java py >}}

{{< paragraph class="language-java" >}}
Add the FlareDB Runner SDK to your `pom.xml`:
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
  <groupId>com.flare-db</groupId>
  <artifactId>flaredb-runner</artifactId>
  <version>0.3.2</version>
</dependency>
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Set `FlareRunner` as the runner and configure the FlareDB instance and application JAR in your pipeline options:
{{< /paragraph >}}

{{< highlight java >}}
WordCountPipelineOptions options =
    PipelineOptionsFactory.fromArgs(args).as(WordCountPipelineOptions.class);

options.setRunner(FlareRunner.class);
options.setJobEndpoint("127.0.0.1:8099");
options.setUberJar("build/libs/wordcount-0.2.0-all.jar");

Pipeline pipeline = Pipeline.create(options);
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Alternatively, you can pass these as CLI arguments while running the pipeline:
{{< /paragraph >}}

{{< highlight java >}}
./gradlew :wordcount:run --args="\
  --runner=FlareRunner \
  --jobEndpoint=127.0.0.1:8099 \
  --uberJar=build/libs/wordcount-0.2.0-all.jar"
{{< /highlight >}}

{{< paragraph class="language-java" >}}
Check out the full Java [WordCount example](https://github.com/flare-db/flare-db/tree/main/example/wordcount/src/main/java/com/flaredb/example) pipeline.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
Create and activate a virtual environment, then install the `flaredb-runner` package. It includes the `apache-beam` dependency.
{{< /paragraph >}}

{{< highlight py >}}
python3 -m venv .venv
source .venv/bin/activate
pip install flaredb-runner
{{< /highlight >}}

{{< paragraph class="language-py" >}}
Set `FlareRunner` as the runner:
{{< /paragraph >}}

{{< highlight py >}}
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from flaredb_runner.flare_runner import FlareRunner

pipeline_options = PipelineOptions(
    job_endpoint="127.0.0.1:8099",
)

with beam.Pipeline(runner=FlareRunner(), options=pipeline_options) as p:
{{< /highlight >}}

{{< paragraph class="language-py" >}}
Run the pipeline:
{{< /paragraph >}}

{{< highlight py >}}
python3 wordcount.py
{{< /highlight >}}

{{< paragraph class="language-py" >}}
Check out the full Python [wordcount example](https://github.com/flare-db/flare-db/blob/main/example/python/wordcount.py) pipeline.
{{< /paragraph >}}


## 5. Stop FlareDB instance

After executing pipelines, run this command to stop FlareDB instance

```bash
flare down
```

## Pipeline options

The FlareDB Runner is configured through the following pipeline options:

<table class="table table-bordered">
<tr>
  <th>Option</th>
  <th>Usage</th>
  <th>Description</th>
</tr>
<tr>
  <td>Runner</td>
  <td><code>setRunner(FlareRunner.class)</code></td>
  <td>Pipeline runner.</td>
</tr>
<tr>
  <td>Job endpoint</td>
  <td><code>setJobEndpoint("host:port")</code></td>
  <td>URL of the FlareDB job service. Defaults to <code>127.0.0.1:8099</code>.</td>
</tr>
<tr>
  <td>Uber JAR</td>
  <td><code>setUberJar("/path/to/app.jar")</code></td>
  <td>Path to the fat JAR staged to workers.</td>
</tr>
<tr>
  <td>Job name</td>
  <td><code>setJobName("my-job")</code></td>
  <td>Name of the submitted job.</td>
</tr>
</table>

## Next steps

- Browse the [FlareDB documentation](https://docs.flare-db.com/).
- See the [Capability Matrix](https://docs.flare-db.com/compatibility) for supported Beam features.
- Try more [examples](https://github.com/flare-db/flare-db/tree/main/example) in the FlareDB repository.
- Report bugs or request features in the [FlareDB issue tracker](https://github.com/flare-db/flare-db/issues).
- Contributions are welcome. See the [Contributing Guide](https://github.com/flare-db/flare-db/blob/main/CONTRIBUTING.md) to get started.

FlareDB is released under the Apache License 2.0.
