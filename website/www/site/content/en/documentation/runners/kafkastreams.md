---
type: runners
title: "Kafka Streams Runner"
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

# Kafka Streams Runner

The Kafka Streams Runner executes Beam pipelines on [Kafka
Streams](https://kafka.apache.org/documentation/streams/), by translating a pipeline into a Kafka
Streams topology.

What distinguishes it from the other runners is that Kafka Streams is a library rather than a
cluster. There is no job manager and no resource manager to operate: an application is an ordinary
JVM process that reads from and writes to Kafka, and scaling it means starting more copies of that
process. Fault tolerance, state, and exactly-once processing come from Kafka itself — from consumer
groups, changelog topics, and transactions.

That makes it worth considering if you already run Kafka and want Beam's programming model without
introducing a second distributed system to operate.

## The runner is experimental

**The Kafka Streams Runner is experimental.** It executes a meaningful subset of the Beam model
correctly, and the parts it does support are covered by Beam's own `@ValidatesRunner` suite, but
several capabilities that are core to the model are not implemented yet. Read [what is not
supported](#what-is-not-supported-yet) before choosing it for anything real.

It is also aimed squarely at streaming. A pipeline over bounded data will run, but there are more
efficient choices for batch work; this runner exists for pipelines that do not end.

## Running a pipeline

The runner is portable: it executes user code over the Fn API, in an SDK harness, so a pipeline goes
to a job server rather than being run directly. You do not have to start one yourself.

### From Java

Select `KafkaStreamsRunner` and point it at your Kafka cluster:

```
--runner=KafkaStreamsRunner \
--bootstrapServers=localhost:9092 \
--applicationId=my-beam-pipeline
```

With no `jobEndpoint` set, the runner starts a job server of its own on a dynamic port, submits to
it, and shuts it down when the pipeline finishes. Setting `--jobEndpoint` instead submits to a job
server you are already running.

### From Python

Select `KafkaStreamsRunner` there too. The Python runner builds the job server jar if it has to,
starts it, and stops it when the pipeline finishes:

```
python my_pipeline.py \
    --runner=KafkaStreamsRunner \
    --bootstrap_servers=localhost:9092 \
    --application_id=my-beam-pipeline
```

The SDK harness runs in `LOOPBACK` mode by default, so a local run needs no Docker. Building the jar
takes a while the first time; `--kafka_streams_job_server_jar` points at a prebuilt one, which
`./gradlew :runners:kafka-streams:job-server:shadowJar` produces.

### Against a job server you are already running

Start one, which listens on `localhost:8099` by default:

```
./gradlew :runners:kafka-streams:runJobServer
```

Then point a pipeline at it instead of letting the runner start its own. From Java:

```
--runner=KafkaStreamsRunner \
--jobEndpoint=localhost:8099 \
--bootstrapServers=localhost:9092 \
--applicationId=my-beam-pipeline
```

and from Python, where the option names are the same in snake case:

```
--runner=PortableRunner \
--job_endpoint=localhost:8099 \
--bootstrap_servers=localhost:9092 \
--application_id=my-beam-pipeline
```

The application id has no default and must be set. It becomes the Kafka Streams `application.id`,
which is the identity of the consumer group and of the runner's internal topics, so two different
pipelines sharing one would interfere with each other.

## Pipeline options

Named as Java spells them below; from Python the same options are in snake case, so
`internalParallelism` is `--internal_parallelism`.

| Option | Default | Description |
| --- | --- | --- |
| `bootstrapServers` | `localhost:9092` | Kafka brokers the application connects to. |
| `applicationId` | *(required)* | Kafka Streams `application.id`. Must be unique per pipeline. |
| `internalParallelism` | `1` | Partitions for the internal topics the runner creates, which is the parallelism the shuffled parts of a pipeline can reach. |
| `topicReplicationFactor` | `1` | Replication factor for those topics. |
| `maxBundleSize` | `1000` | Elements per bundle, and elements taken per poll of an unbounded source. |
| `maxBundleTimeMs` | `1000` | Intended cap on how long a bundle may stay open. **Not applied yet** — see below. |
| `readCheckpointNumBundles` | `10` | Polls of an unbounded source between stores of its checkpoint mark. Larger values replay more after a restart. |
| `stateDir` | temp directory | Where Kafka Streams keeps local state. |

### Topics the runner creates

The runner shuffles through topics it names itself and creates before starting: a bootstrap topic
per `Impulse` and per source, and a repartition topic per `GroupByKey`. They carry a `__beam_`
prefix. Bootstrap topics always have one partition; repartition topics get `internalParallelism`,
which is what sets how many instances the parts of the pipeline behind a shuffle run across.

Topics the pipeline itself reads or writes are never created implicitly.

## What is supported

* **Reading** — bounded and unbounded sources, through the primitive `Read`.
* **ParDo** — stateless, including multiple outputs.
* **GroupByKey**, and `Combine` through its GroupByKey expansion.
* **Windowing** — global, fixed and sliding windows, with the default trigger, allowed lateness, and
  timestamp combiners. Windowing and triggering run through Beam's own `ReduceFnRunner`, backed by
  Kafka Streams state and timers.
* **Flatten**, **Redistribute**.
* **Metrics** — user counters and distributions reported by the SDK harness surface as
  `MetricResults`.
* **Exactly-once processing**, via Kafka transactions (`exactly_once_v2`).

Because the runner is portable and reads the language-neutral pipeline proto, a pipeline built in
any Beam SDK should translate, provided it stays inside the subset above. Only the Java SDK has been
exercised so far.

## What is not supported yet

These are core parts of the Beam model that the runner does not implement. Each is a real gap rather
than a decision, and each is tracked:

* **Side inputs** ([#39628](https://github.com/apache/beam/issues/39628)).
* **Stateful `ParDo` and user timers** ([#39629](https://github.com/apache/beam/issues/39629)) —
  including timer families, looping timers
  and processing-time timers.
* **Merging windows**, so session windows do not work, and **custom `WindowFn`s**
    ([#39630](https://github.com/apache/beam/issues/39630)). The standard windows travel as URNs the
  runner interprets directly; one the
  user wrote themselves would have to run in the SDK harness, which is not wired up.
* **Splittable `DoFn`**, bounded or unbounded
  ([#39631](https://github.com/apache/beam/issues/39631)).
* **`TestStream`** ([#39632](https://github.com/apache/beam/issues/39632)).
* **Reading a source in parallel**
  ([#39626](https://github.com/apache/beam/issues/39626)). A source is split into exactly one part
  and read by a single reader. A source that insists on splitting further is rejected at
  translation rather than having its extra splits silently dropped.
* **A time bound on bundles** ([#39633](https://github.com/apache/beam/issues/39633)).
  `maxBundleTimeMs` is accepted but has no effect:
  closing a bundle from a wall-clock punctuator duplicated output against a real broker, and the
  cause is not yet understood. Bundles are bounded by element count and closed on watermarks.
* **`finalizeCheckpoint`** ([#39634](https://github.com/apache/beam/issues/39634)) is not called on
  an unbounded source's checkpoint mark,
  so sources that rely on finalization to acknowledge data will not see it.
* **Committed metrics** ([#39635](https://github.com/apache/beam/issues/39635)) — only attempted
  values are reported.

## How it works

A Beam pipeline arrives as a proto and is translated into a Kafka Streams `Topology`. Fused stages
of user code become processors that execute that code in an SDK harness over the Fn API; a
`GroupByKey` becomes a repartition topic plus a stateful processor; and the elements flowing between
them carry either data or a watermark report.

Watermarks are the part with no direct Kafka Streams equivalent. Kafka Streams tracks stream-time,
which only advances when data arrives, whereas Beam needs a watermark that can advance on an idle
stream and that reflects every upstream instance. The runner therefore propagates its own watermark
reports alongside the data: a transform aggregates the reports of everything upstream of it, holds
until every partition of every upstream transform has reported, and only then lets its own watermark
advance.

For the full design, see the [design
document](https://docs.google.com/document/d/1BBMURhSG4SxPcvvnKMTrmnKCr_jhXL6R4TBDBW7zsy8/edit) and
the tracking issue, [#18479](https://github.com/apache/beam/issues/18479).
