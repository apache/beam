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

# TypeScript Beam SDK

A library for writing [Apache Beam](https://beam.apache.org/)
pipelines in Typescript.

As well as being a fully-functioning SDK, it serves as a cleaner, more modern
template for building SDKs in other languages
(see README-dev.md for more details).


## Getting started

The Typescript SDK can be installed with

```
npm install apache_beam
```

Due to its extensive use of cross-language transforms, it is recommended that
Python 3 and Java be available on the system as well.

A fully working setup is provided as a clonable
[starter project on github](https://github.com/apache/beam-starter-typescript).


### Running a pipeline

Beam pipelines can be run on a variety of
[runners](https://beam.apache.org/documentation/#runners).
The typical way to create a runner is with
`beam.runners.runner.create_runner({runner: "runnerType", ...})`,
as seen in the [wordcount example](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/examples/wordcount.ts).

After building, to run locally one can execute:

```
node path/to/main.js --runner=direct
```

To run against Flink, where the local infrastructure is automatically
downloaded and set up:

```
node path/to/main.js --runner=flink
```

To run on Dataflow:

```
node path/to/main.js \
    --runner=dataflow \
    --project=${PROJECT_ID} \
    --tempLocation=gs://${GCS_BUCKET}/wordcount-js/temp --region=${REGION}
```


## API

We generally try to apply the concepts from the Beam API in a TypeScript
idiomatic way, but it should be noted that few of the initial developers
have extensive (if any) JavaScript/TypeScript development experience, so
feedback is greatly appreciated.

In addition, some notable departures are taken from the traditional SDKs:

* We take a "relational foundations" approach, where
[schema'd data](https://docs.google.com/document/d/1tnG2DPHZYbsomvihIpXruUmQ12pHGK0QIvXS1FOTgRc/edit#heading=h.puuotbien1gf)
is the primary way to interact with data, and we generally eschew the key-value
requiring transforms in favor of a more flexible approach naming fields or
expressions. JavaScript's native Object is used as the row type.

* As part of being schema-first we also de-emphasize Coders as a first-class
concept in the SDK, relegating it to an advanced feature used for interop.
Though we can infer schemas from individual elements, it is still TBD to
figure out if/how we can leverage the type system and/or function introspection
to regularly infer schemas at construction time. A fallback coder using BSON
encoding is used when we don't have sufficient type information.

* We have added additional methods to the PCollection object, notably `map`
and `flatmap`, [rather than only allowing apply](https://www.mail-archive.com/dev@beam.apache.org/msg06035.html).
In addition, `apply` can accept a function argument `(PCollection) => ...` as
well as a PTransform subclass, which treats this callable as if it were a
PTransform's expand.

* In the other direction, we have eliminated the
[problematic Pipeline object](https://s.apache.org/no-beam-pipeline)
from the API, instead providing a `Root` PValue on which pipelines are built,
and invoking run() on a Runner.  We offer a less error-prone `Runner.run`
which finishes only when the pipeline is completely finished as well as
`Runner.runAsync` which returns a handle to the running pipeline.

* Rather than introduce PCollectionTuple, PCollectionList, etc. we let PValue
literally be an
[array or object with PValue values](https://github.com/robertwb/beam-javascript/blob/de4390dd767f046903ac23fead5db333290462db/sdks/node-ts/src/apache_beam/pvalue.ts#L116)
which transforms can consume or produce.
These are applied by wrapping them with the `P` operator, e.g.
`P([pc1, pc2, pc3]).apply(new Flatten())`.

* Like Python, `flatMap` and `ParDo.process` return multiple elements by
yielding them from a generator, rather than invoking a passed-in callback.
TBD how to output to multiple distinct PCollections.
There is currently an operation to split a PCollection into multiple
PCollections based on the properties of the elements, and
we may consider using a callback for side outputs.

* The `map`, `flatMap`, and `ParDo.process` methods take an additional
(optional) context argument, which is similar to the keyword arguments
used in Python. These are javascript objects whose members may be constants
(which are passed as is) or special DoFnParam objects which provide getters to
element-specific information (such as the current timestamp, window,
or side input) at runtime.

* Rather than introduce multiple-output complexity into the map/do operations
themselves, producing multiple outputs is done by following with a new
`Split` primitive that takes a
`PCollection<{a?: AType, b: BType, ... }>` and produces an object
`{a: PCollection<AType>, b: PCollection<BType>, ...}`.

* JavaScript supports (and encourages) an asynchronous programing model, with
many libraries requiring use of the async/await paradigm.
As there is no way (by design) to go from the asynchronous style back to
the synchronous style, this needs to be taken into account
when designing the API.
We currently offer asynchronous variants of `PValue.apply(...)` (in addition
to the synchronous ones, as they are easier to chain) as well as making
`Runner.run` asynchronous. TBD to do this for all user callbacks as well.

An example pipeline can be found at https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/examples/wordcount.ts
and more documentation can be found in the [beam programming guide](https://beam.apache.org/documentation/programming-guide/).
