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

# Running Python pipelines on a local Flink cluster

This guide describes a contributor workflow for validating Python Beam pipelines
against a real local Flink standalone cluster. It is useful when embedded Flink
is not enough, for example when validating streaming source behavior, checkpoint
boundaries, or runner-visible job state in the Flink dashboard.

The commands assume a Unix shell (Linux, macOS, or WSL2 on Windows) with `curl`,
`tar`, and `java` on the `PATH`.

* [What this setup validates](#what-this-setup-validates)
* [Prerequisites](#prerequisites)
* [Start a local Flink cluster](#start-a-local-flink-cluster)
* [Run a Beam Python pipeline](#run-a-beam-python-pipeline)
* [Troubleshooting](#troubleshooting)
* [Stop the cluster](#stop-the-cluster)

## What this setup validates

This setup runs three separate processes:

1. A Flink standalone cluster, consisting of a JobManager and a TaskManager.
1. A Beam Flink Job Server, started by the Python `FlinkRunner`.
1. A Python SDK harness, using `--environment_type=LOOPBACK` for local
   development.

The Flink dashboard at `http://localhost:8081` shows the submitted Beam jobs.
This is different from embedded Flink mode, where the cluster is started only
for the lifetime of one job and is not useful for manual dashboard inspection.

## Prerequisites

Install or prepare the following:

* Docker Desktop (optional), only for the alternative method of obtaining the
  Flink distribution.
* A Unix shell: Linux, macOS, or WSL2 on Windows.
* Java 11 on the `PATH`.
* A Python environment with the Beam SDK dependencies installed.
* A Beam source checkout for the Python code under test.
* A Flink 1.20 Job Server jar built from the same Beam checkout when validating
  unreleased Beam changes.

For a source-built Job Server jar, run this command from the Beam checkout:

```sh
./gradlew :runners:flink:1.20:job-server:shadowJar
```

The jar is written under:

```text
runners/flink/1.20/job-server/build/libs/
```

## Start a local Flink cluster

Use a Flink distribution whose minor version matches a Flink version supported
by your Beam version. See the [Flink Version Compatibility](https://beam.apache.org/documentation/runners/flink/#flink-version-compatibility)
table in the Flink Runner documentation, and confirm the exact patch version on
the [Flink downloads page](https://flink.apache.org/downloads.html). This guide
uses Flink 1.20.

Download and unpack the binary distribution:

```sh
FLINK_VERSION=1.20.1
curl -fLO "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
tar -xzf "flink-${FLINK_VERSION}-bin-scala_2.12.tgz" -C "$HOME"
export FLINK_HOME="$HOME/flink-${FLINK_VERSION}"
```

Ensure these settings exist in `$FLINK_HOME/conf/config.yaml`:

```yaml
jobmanager.rpc.address: localhost
rest.address: localhost
taskmanager.numberOfTaskSlots: 2
```

Start the cluster. The JobManager and TaskManager run as background daemons:

```sh
"$FLINK_HOME/bin/start-cluster.sh"
```

Verify that the JobManager and TaskManager are available:

```sh
curl -fsS http://localhost:8081/overview
```

Expected output includes one TaskManager and two slots:

```json
{"taskmanagers":1,"slots-total":2,"slots-available":2,"jobs-running":0}
```

You can also open the Flink dashboard in a browser:

```text
http://localhost:8081
```

### Alternative: extract Flink from the Docker image

If a direct download is not available, copy the distribution out of the Flink
Docker image with `docker cp`:

```sh
docker create --name flink-dist flink:1.20
docker cp flink-dist:/opt/flink "$HOME/flink-1.20"
docker rm flink-dist
export FLINK_HOME="$HOME/flink-1.20"
```

A distribution copied out of a Docker image can contain the container hostname in
`conf/config.yaml`; see [Troubleshooting](#troubleshooting).

## Run a Beam Python pipeline

For local Python development, use `FlinkRunner`, point it at the standalone
cluster, and use `LOOPBACK` so the Python SDK harness runs in the local process.

Use a source checkout on `PYTHONPATH` when validating unreleased Python changes.
Set paths for your environment:

```sh
export BEAM_CHECKOUT="$HOME/beam"
export PYTHON="$HOME/beamenv/bin/python"
export FLINK_JOB_SERVER_JAR="$(find "$BEAM_CHECKOUT/runners/flink/1.20/job-server/build/libs" \
  -name 'beam-runners-flink-1.20-job-server-*.jar' | head -n 1)"
```

Run a small pipeline:

```sh
printf 'to be or not to be\nbeam runs on flink\n' > /tmp/beam-flink-input.txt

PYTHONPATH="$BEAM_CHECKOUT/sdks/python" "$PYTHON" -m apache_beam.examples.wordcount \
  --runner=FlinkRunner \
  --flink_master=localhost:8081 \
  --flink_version=1.20 \
  --flink_job_server_jar="$FLINK_JOB_SERVER_JAR" \
  --environment_type=LOOPBACK \
  --input=/tmp/beam-flink-input.txt \
  --output=/tmp/beam-flink-counts
```

For released Beam, omit `--flink_job_server_jar` and the `PYTHONPATH` prefix; the
`FlinkRunner` downloads a Job Server matching `--flink_version` automatically. The
source checkout and built jar are only needed to test unreleased changes.

Check the dashboard or REST API after the run:

```sh
curl -fsS http://localhost:8081/jobs/overview
```

The job should be `FINISHED`.

## Troubleshooting

If the TaskManager does not register, check `$FLINK_HOME/conf/config.yaml`.
When a distribution is copied out of a Docker image, the file might contain the
container hostname. Replace it with:

```yaml
jobmanager.rpc.address: localhost
```

If a Python job fails on native Windows with an invalid path containing `:`,
run the Python driver and Job Server from WSL2. Some staged artifact names used
by the portable runner are valid on Linux but invalid as native Windows file
names.

On WSL2, keep at least one shell open in the distribution while the cluster runs.
Closing the last shell can stop the distribution and its background daemons.

If the job starts but the Python transforms do not execute, check the
environment type. `LOOPBACK` is intended for local development. For a remote
or multi-machine Flink cluster, use a containerized environment instead.

## Stop the cluster

Stop the local cluster when you finish collecting results:

```sh
"$FLINK_HOME/bin/stop-cluster.sh"
```
