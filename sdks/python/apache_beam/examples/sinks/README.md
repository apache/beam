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

# Examples of writing to Sinks

This module contains example pipelines that use the [Beam IO connectors](https://beam.apache.org/documentation/io/connectors/) also known as Sinks to write in streaming and batch.

## Batch

test_write_bounded.py - a simple pipeline taking a bounded PCollection
as input using the [Create](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Create)
 transform (useful for testing) and writing it to files using multiple IOs.

### Running the pipeline

To run the pipeline locally:

```sh
python -m apache_beam.examples.sinks.test_write_bounded
```

## Streaming

Two example pipelines that use 2 different approches for creating the input.

test_write_unbounded.py uses [TestStream](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/testing/TestStream.html),
a method where you can control when data arrives and how watermark advances.
This is especially useful in unit tests.

test_periodicimpulse.py uses [PeriodicImpulse](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.periodicsequence.html#apache_beam.transforms.periodicsequence.PeriodicImpulse),
a method useful to test pipelines in realtime. You can run it to Dataflow as well.

### Running the pipeline

To run the pipelines locally:

```sh
python -m apache_beam.examples.sinks.test_write_unbounded
```

```sh
python -m apache_beam.examples.sinks.test_periodicimpulse
```