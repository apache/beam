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

This is the start of a fully functioning JavaScript (actually, TypeScript) SDK.
There are two distinct aims with this SDK:

1. Tap into the large (and relatively underserved, by existing data processing
frameworks) community of JavaScript developers with a native SDK targeting this language.

1. Develop a new SDK which can serve both as a proof of concept and reference
that highlights the (relative) ease of porting Beam to new languages,
a differentiating feature of Beam and Dataflow.

To accomplish this, we lean heavily on the portability framework.
For example, we make heavy use of cross-language transforms,
in particular for IOs.
In addition, the direct runner is simply an extension of the worker suitable
for running on portable runners such as the ULR, which will directly transfer
to running on production runners such as Dataflow and Flink.
The target audience should hopefully not be put off by running other-language
code encapsulated in docker images.

## Getting started

To install and test the Typescript SDK from source, you will need `npm` and
`python`. Other requirements can be installed by `npm` later on.

(**Note** that Python is a requirement as it is used to orchestrate Beam
functionality.)

1. First you must clone the Beam repository and go to the `typescript` directory.
```
git checkout https://github.com/apache/beam
cd beam/sdks/typescript/
```

2. Execute a local install of the necessary packages:

```
npm install
```

3. Then run `npm run build` to transpile Typescript files into JS files.

### Development workflows

All of the development workflows (build, test, lint, clean, etc) are defined in
`package.json` and can be run with `npm` commands (e.g. `npm run build`).

### Running a pipeline

The `wordcount.ts` file defines a parameterizable pipeline that can be run
against different runners. You can run it from the transpiled `.js` file
like so:

```
node dist/src/apache_beam/examples/wordcount.js ${PARAMETERS}
```

To run locally:

```
node dist/src/apache_beam/examples/wordcount.js --runner=direct
```

To run against Flink, where the local infrastructure is automatically
downloaded and set up:

```
node dist/src/apache_beam/examples/wordcount.js --runner=flink
```

To run on Dataflow:

```
node dist/src/apache_beam/examples/wordcount.js \
    --runner=dataflow \
    --project=${PROJECT_ID} \
    --tempLocation=gs://${GCS_BUCKET}/wordcount-js/temp --region=${REGION}
```

## TODO

This SDK is a work in progress. In January 2022 we developed the ability to
construct and run basic pipelines (including external transforms and running
on a portable runner) but the following big-ticket items remain.

* Containerization

  * Actually use worker threads for multiple bundles
    (unsure if this is a large benefit, mitigated using sibling workers).

* API

  * There are several TODOs of minor features or design decisions to finalize.

    * Consider using (or supporting) 2-arrays rather than {key, value} objects
      for KVs.

    * Force the second argument of map/flatMap to be an Object, which would lead
    to a less confusing API (vs. Array.map) and clean up the implementation.
    Also add a [do]Filter, and possibly a [do]Reduce?

    * Move away from using classes.

  * Advanced features like state, timers, and SDF.

* Other

  * Relative vs. absoute imports, possibly via setting a base url with a
  `jsconfig.json`.

  * More/better tests, including tests of illegal/unsupported use.

  * Set channel options like `grpc.max_{send,receive}_message_length` as we
  do in other SDKs.

  * Reduce use of `any`.

    * Could use `unknown` in its place where the type is truly unknown.

    * It'd be nice to enforce, maybe re-enable `noImplicitAny: true` in
    tsconfig if we can get the generated proto files to be ignored.

  * Enable a linter like eslint and fix at least the low hanging fruit.

There is probably more; there are many TODOs littered throughout the code.

## Development.

### Getting stared

Install node.js, and then from within `sdks/typescript`.

```
npm install
```

### Running tests

```
npm test
```

### Style

We have adopted prettier which can be run with

```
npx prettier --write .
```
