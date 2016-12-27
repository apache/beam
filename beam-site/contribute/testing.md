---
layout: default
title: 'Beam Testing Guide'
permalink: /contribute/testing/
---

# Beam Testing Documentation

* TOC
{:toc}

## Overview

Apache Beam is a rapidly-maturing software project with a strong
commitment to testing. Consequently, it has many testing-related needs. It
requires precommit tests to ensure code going into the repository meets a
certain quality bar and it requires ongoing postcommit tests to make sure that
more subtle changes which escape precommit are nonetheless caught. This document
outlines how to write tests, which tests are appropriate where, and when tests
are run, with some additional information about the testing systems at the
bottom.

If you’re writing tests, take a look at the testing matrix first, find what you
want to test, then look into the “Scenarios” and “Types” sections below for more
details on those testing types.

## Testing Matrix

### Java SDK

<table>
  <tr>
   <td><strong>Component to Test</strong>
   </td>
   <td><strong>Test Scenario</strong>
   </td>
   <td><strong>Tool to Use</strong>
   </td>
   <td><strong>Link to Example</strong>
   </td>
   <td><strong>Type</strong>
   </td>
   <td><strong>Runs In</strong>
   </td>
  </tr>
  <tr>
   <td>DoFn
   </td>
   <td>Correctness on one/few bundles
   </td>
   <td>DoFnTester
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIOTest.java#L1325">BigQueryIOTest</a>
   </td>
   <td>Unit
   </td>
   <td>Precommit, Postcommit
   </td>
  </tr>
  <tr>
   <td>BoundedSource
   </td>
   <td>Correctly Reads Input
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/SourceTestUtils.java#L128">SourceTestUtils.readFromSource</a>
   </td>
   <td><a href="https://github.com/apache/beam/blob/84a0dd1714028370befa80dea16f720edce05252/sdks/java/core/src/test/java/org/apache/beam/sdk/io/TextIOTest.java#L972">TextIOTest</a>
   </td>
   <td>Unit
   </td>
   <td>Precommit, Postcommit
   </td>
  </tr>
  <tr>
   <td>
   </td>
   <td>Correct Initial Splitting
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/SourceTestUtils.java#L201">SourceTestUtils.assertSourcesEqualReferenceSource</a>
   </td>
   <td><a href="https://github.com/apache/beam/blob/8b1e64a668489297e11926124c4eee6c8f69a3a7/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIOTest.java#L339">BigtableTest</a>
   </td>
   <td>Unit
   </td>
   <td>Precommit, Postcommit
   </td>
  </tr>
  <tr>
   <td>
   </td>
   <td>Correct Dynamic Splitting
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/SourceTestUtils.java#L541">SourceTestUtils. assertSplitAtFractionExhaustive</a>
   </td>
   <td><a href="https://github.com/apache/beam/blob/84a0dd1714028370befa80dea16f720edce05252/sdks/java/core/src/test/java/org/apache/beam/sdk/io/TextIOTest.java#L1021">TextIOTest</a>
   </td>
   <td>Unit
   </td>
   <td>Precommit, Postcommit
   </td>
  </tr>
  <tr>
   <td>Transform
   </td>
   <td>Correctness
   </td>
   <td>@NeedsRunner Test
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/transforms/ParDoTest.java#L1199">ParDoTest</a>
   </td>
   <td>@NeedsRunner
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td>Example Pipeline
   </td>
   <td>Verify Behavior on Each Runner
   </td>
   <td>E2E Test
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/examples/java/src/test/java/org/apache/beam/examples/WordCountIT.java#L76">WordCountIT</a>
   </td>
   <td>E2E
   </td>
   <td>Postcommit (Except WordCountIT)
   </td>
  </tr>
  <tr>
   <td>Source/Sink with external resource
   </td>
   <td>External Resource Faked
   </td>
   <td>Unit / @NeedsRunner Test
   </td>
   <td><a href="https://github.com/apache/beam/blob/84a0dd1714028370befa80dea16f720edce05252/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIOTest.java#L646">FakeBigtableService in BigtableTest</a>
   </td>
   <td>Unit / @NeedsRunner
   </td>
   <td>Precommit / Postcommit
   </td>
  </tr>
  <tr>
   <td>
   </td>
   <td>Real Interactions With External Resource
   </td>
   <td>E2E Test
   </td>
   <td><a href="https://github.com/apache/beam/blob/84a0dd1714028370befa80dea16f720edce05252/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableReadIT.java#L40">BigtableReadIT</a>
   </td>
   <td>E2E
   </td>
   <td>Postcommit
   </td>
  </tr>
  <tr>
   <td>Runner
   </td>
   <td>Correctness
   </td>
   <td>E2E Test, <a href="https://github.com/apache/beam/blob/master/runners/pom.xml#L47">@RunnableonService</a>
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/examples/java/src/test/java/org/apache/beam/examples/WordCountIT.java#L78">WordCountIT</a>, <a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/transforms/ParDoTest.java">ParDoTest</a>
   </td>
   <td>E2E, @RunnableonService
   </td>
   <td>Postcommit
   </td>
  </tr>
  <tr>
   <td>Coders
   </td>
   <td>Encoding/decoding elements
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/CoderProperties.java">CoderProperties</a>
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/coders/NullableCoderTest.java">NullableCoderTest</a>
   </td>
   <td>Unit
   </td>
   <td>Precommit / Postcommit
   </td>
  </tr>
  <tr>
   <td>
   </td>
   <td>Serialization/deserialization of Coder
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/CoderProperties.java">CoderProperties</a>
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/coders/NullableCoderTest.java">NullableCoderTest</a>
   </td>
   <td>Unit
   </td>
   <td>Precommit / Postcommit
   </td>
  </tr>
  <tr>
   <td>
   </td>
   <td>Sizing of elements
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/CoderProperties.java">CoderProperties</a>
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/coders/NullableCoderTest.java">NullableCoderTest</a>
   </td>
   <td>Unit
   </td>
   <td>Precommit / Postcommit
   </td>
  </tr>
  <tr>
   <td>
   </td>
   <td>Deterministic
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/CoderProperties.java">CoderProperties</a>
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/coders/NullableCoderTest.java">NullableCoderTest</a>
   </td>
   <td>Unit
   </td>
   <td>Precommit / Postcommit
   </td>
  </tr>
  <tr>
   <td>
   </td>
   <td>Structural value equality
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/testing/CoderProperties.java">CoderProperties</a>
   </td>
   <td><a href="https://github.com/apache/beam/blob/master/sdks/java/core/src/test/java/org/apache/beam/sdk/coders/NullableCoderTest.java">NullableCoderTest</a>
   </td>
   <td>Unit
   </td>
   <td>Precommit / Postcommit
   </td>
  </tr>
</table>

### Python SDK

The Python SDK is currently under development on a feature branch. We have initial
postcommit tests by a Jenkins build; precommit testing and a full testing 
matrix will be coming soon.

## Testing Scenarios

With the tools at our disposal, we have a good set of utilities which we can use
to verify Beam correctness. To ensure an ongoing high quality of code, we use
precommit and postcommit testing.

### Precommit

For precommit testing, Beam uses
[Jenkins](https://builds.apache.org/view/Beam/),
[Travis](http://travis-ci.org/apache/incubator-beam), and a code coverage tool
called [Coveralls](https://coveralls.io/github/apache/incubator-beam), hooked up
to [Github](https://github.com/apache/beam), to ensure that pull
requests meet a certain quality bar. These precommits verify correctness via two
of the below testing tools: unit tests (with coverage monitored by Coveralls)
and E2E tests. We run the full slate of unit tests in precommit, ensuring
correctness at a basic level, and then run the WordCount E2E test in both batch
and streaming (coming soon!) against each supported SDK / runner combination as
a smoke test, to verify that a basic level of functionality exists. We think
that this hits the appropriate tradeoff between a desire for short (ideally
\<30m) precommit times and a desire to verify that pull requests going into Beam
function in the way in which they are intended.

Precommit tests are kicked off when a user makes a Pull Request against the
apache/incubator-beam repository and the Travis, Jenkins, and Coveralls statuses
are displayed at the bottom of the pull request page. Clicking on “Details” will
open the status page in the selected tool; there, test status and output can be
viewed.

### Postcommit

Running in postcommit removes as stringent of a time constraint, which gives us
the ability to do some more comprehensive testing. In postcommit we have a test
suite running the RunnableOnService tests against each supported runner, and
another for running the full set of E2E tests against each runner.
Currently-supported runners are Dataflow, Flink, Spark, and Gearpump, with
others soon to follow. Work is ongoing to enable Flink, Spark, and Gearpump in
the E2E framework, with full support targeted for end of August 2016. Postcommit
tests run periodically, with timing defined in their Jenkins configurations.

Adding new postcommit E2E tests is generally as easy as adding a \*IT.java file
to the repository - Failsafe will notice it and run it - but if you want to do
more interesting things, take a look at
[WordCountIT.java](https://github.com/apache/beam/blob/master/examples/java/src/test/java/org/apache/beam/examples/WordCountIT.java).

Postcommit test results can be found in
[Jenkins](https://builds.apache.org/view/Beam/).

## Testing Types

### Unit

Unit tests are, in Beam as everywhere else, the first line of defense in
ensuring software correctness. As all of the contributors to Beam understand the
importance of testing, Beam has a robust set of unit tests, as well as testing
coverage measurement tools, which protect the codebase from simple to moderate
breakages. Beam Java unit tests are written in JUnit.

### RunnableOnService (Working Title)

RunnableOnService tests contain components of both component and end-to-end
tests. They fulfill the typical purpose of a component test - they are meant to
test a well-scoped piece of Beam functionality or the interactions between two
such pieces and can be run in a component-test-type fashion against the
DirectRunner. Additionally, they are built with the ability to run in an
end-to-end fashion against a runner, allowing them to verify not only core Beam
functionality, but runner functionality as well. They are more lightweight than
a traditional end-to-end test and, because of their well-scoped nature, provide
good signal as to what exactly is working or broken against a particular runner.

The name “RunnableOnService” is an artifact of when Beam was still the Google
Cloud Dataflow SDK and [will be
changing](https://issues.apache.org/jira/browse/BEAM-655) to something more
indicative of its use in the coming months.

### E2E

End-to-End tests are meant to verify at the very highest level that the Beam
codebase is working as intended. Because they are implemented as a thin wrapper
around existing pipelines, they can be used to prove that the core Beam
functionality is available. They will be used to verify runner correctness, but
they can also be used for IO connectors and other core functionality.

## Testing Systems

### E2E Testing Framework

The Beam end-to-end testing framework is a framework designed in a
runner-agnostic fashion to exercise the entire lifecycle of a Beam pipeline. We
run a pipeline as a user would and allow it to run to completion in the same
way, verifying after completion that it behaved how we expected. Using pipelines
from the Beam examples, or custom-built pipelines, the framework will provide
hooks during several pipeline lifecycle events, e.g., pipeline creation,
pipeline success, and pipeline failure, to allow verification of pipeline state.

The E2E testing framework is currently built to hook into the [Maven Failsafe
Integration Test
plugin](http://maven.apache.org/surefire/maven-failsafe-plugin/), which means it
is tightly integrated with the overall build process. Once it is determined how
Python and other future languages will integrate into the overall build/test
system (via Maven or otherwise) we will adjust this. The framework provides a
wrapper around actual Beam pipelines, enabling those pipelines to be run in an
environment which facilitates verification of pipeline results and details.

Verifiers include:

*   Output verification. Output verifiers ensure that the pipeline has produced
    the expected output. Current verifiers check text-based output, but future
    verifiers could support other output such as BigQuery and Datastore.
*   Aggregator verification. Aggregator verifiers ensure that the user-defined
    aggregators present in the pipelines under test finish in the expected
    state.

The E2E framework will support running on various different configurations of
environments. We currently provide the ability to run against the DirectRunner,
against a local Spark instance, a local Flink instance, and against the Google
Cloud Dataflow service.

### RunnableOnService Tests

RunnableOnService tests are tests built to use the Beam TestPipeline class,
which enables test authors to write simple functionality verification. They are
meant to use some of the built-in utilities of the SDK, namely PAssert, to
verify that the simple pipelines they run end in the correct state.

