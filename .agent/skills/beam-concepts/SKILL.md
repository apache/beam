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

name: beam-concepts
description: Explains core Apache Beam programming model concepts including PCollections, PTransforms, Pipelines, and Runners. Use when learning Beam fundamentals or explaining pipeline concepts.
---

# Apache Beam Core Concepts

## The Beam Model
Evolved from Google's MapReduce, FlumeJava, and Millwheel projects. Originally called the "Dataflow Model."

## Key Abstractions

### Pipeline
A Pipeline encapsulates the entire data processing task, including reading, transforming, and writing data.

```java
// Java
Pipeline p = Pipeline.create(options);
p.apply(...)
 .apply(...)
 .apply(...);
p.run().waitUntilFinish();
```

```python
# Python
with beam.Pipeline(options=options) as p:
    (p | 'Read' >> beam.io.ReadFromText('input.txt')
       | 'Transform' >> beam.Map(process)
       | 'Write' >> beam.io.WriteToText('output'))
```

### PCollection
A distributed dataset that can be bounded (batch) or unbounded (streaming).

#### Properties
- **Immutable** - Once created, cannot be modified
- **Distributed** - Elements processed in parallel
- **May be bounded or unbounded**
- **Timestamped** - Each element has an event timestamp
- **Windowed** - Elements assigned to windows

### PTransform
A data processing operation that transforms PCollections.

```java
// Java
PCollection<String> output = input.apply(MyTransform.create());
```

```python
# Python
output = input | 'Name' >> beam.ParDo(MyDoFn())
```

## Core Transforms

### ParDo
General-purpose parallel processing.

```java
// Java
input.apply(ParDo.of(new DoFn<String, Integer>() {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<Integer> out) {
        out.output(element.length());
    }
}));
```

```python
# Python
class LengthFn(beam.DoFn):
    def process(self, element):
        yield len(element)

input | beam.ParDo(LengthFn())
# Or simpler:
input | beam.Map(len)
```

### GroupByKey
Groups elements by key.

```java
PCollection<KV<String, Integer>> input = ...;
PCollection<KV<String, Iterable<Integer>>> grouped = input.apply(GroupByKey.create());
```

### CoGroupByKey
Joins multiple PCollections by key.

### Combine
Combines elements (sum, mean, etc.).

```java
// Global combine
input.apply(Combine.globally(Sum.ofIntegers()));

// Per-key combine
input.apply(Combine.perKey(Sum.ofIntegers()));
```

### Flatten
Merges multiple PCollections.

```java
PCollectionList<String> collections = PCollectionList.of(pc1).and(pc2).and(pc3);
PCollection<String> merged = collections.apply(Flatten.pCollections());
```

### Partition
Splits a PCollection into multiple PCollections.

## Windowing

### Types
- **Fixed Windows** - Regular, non-overlapping intervals
- **Sliding Windows** - Overlapping intervals
- **Session Windows** - Gaps of inactivity define boundaries
- **Global Window** - All elements in one window (default)

```java
input.apply(Window.into(FixedWindows.of(Duration.standardMinutes(5))));
```

```python
input | beam.WindowInto(beam.window.FixedWindows(300))
```

## Triggers
Control when results are emitted.

```java
input.apply(Window.<T>into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(AfterWatermark.pastEndOfWindow()
        .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(1))))
    .withAllowedLateness(Duration.standardHours(1))
    .accumulatingFiredPanes());
```

## Side Inputs
Additional inputs to ParDo.

```java
PCollectionView<Map<String, String>> sideInput = 
    lookupTable.apply(View.asMap());

mainInput.apply(ParDo.of(new DoFn<String, String>() {
    @ProcessElement
    public void processElement(ProcessContext c) {
        Map<String, String> lookup = c.sideInput(sideInput);
        // Use lookup...
    }
}).withSideInputs(sideInput));
```

## Pipeline Options
Configure pipeline execution.

```java
public interface MyOptions extends PipelineOptions {
    @Description("Input file")
    @Required
    String getInput();
    void setInput(String value);
}

MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
```

## Schema
Strongly-typed access to structured data.

```java
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class User {
    public abstract String getName();
    public abstract int getAge();
}

PCollection<User> users = ...;
PCollection<Row> rows = users.apply(Convert.toRows());
```

## Error Handling

### Dead Letter Queue Pattern
```java
TupleTag<String> successTag = new TupleTag<>() {};
TupleTag<String> failureTag = new TupleTag<>() {};

PCollectionTuple results = input.apply(ParDo.of(new DoFn<String, String>() {
    @ProcessElement
    public void processElement(ProcessContext c) {
        try {
            c.output(process(c.element()));
        } catch (Exception e) {
            c.output(failureTag, c.element());
        }
    }
}).withOutputTags(successTag, TupleTagList.of(failureTag)));

results.get(successTag).apply(WriteToSuccess());
results.get(failureTag).apply(WriteToDeadLetter());
```

## Cross-Language Pipelines
Use transforms from other SDKs.

```python
# Use Java Kafka connector from Python
from apache_beam.io.kafka import ReadFromKafka

result = pipeline | ReadFromKafka(
    consumer_config={'bootstrap.servers': 'localhost:9092'},
    topics=['my-topic']
)
```

## Best Practices
1. **Prefer built-in transforms** over custom DoFns
2. **Use schemas** for type-safe operations
3. **Minimize side inputs** for performance
4. **Handle late data** explicitly
5. **Test with DirectRunner** before deploying
6. **Use TestPipeline** for unit tests
