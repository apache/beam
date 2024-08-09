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

# Prism Deep Dive Walkthrough

The Apache Beam Go SDK Prism Runner is a portable local Beam runner, that aims to be
the best way to test and validate your pipelines outside of production.

In short: It's too hard to make sure your pipeline will work the way you expect.

Runners have different semantics, and implement different feature sets, and 
have different behaviors. They're all bigger than expected. Some require
tuning to run properly locally. Some requires payment.

We largely operate as though a user has synced to the repo, which shouldn't be the case for all users.

# Runner Capabilities

Dataflow has two different execution modes, which have their own execution characteristics.
Dataflow requires a cloud connection, and costs money.

Dataflow doesn't support ProcessingTime in Batch.
Flink doesn't support ProcessingTime, and requires specific Java configuration.
Java Direct runner only works for Java, and requires specific Java configuration.
Python Direct/Portable runner doesn't handle streaming, and requires specific Python configuration.
Go Direct Runner has never received much development, because the Go SDK was built
against Dataflow and it's google internal counterpart, Flume.

People references the Direct runner, but don't clarify which one, because language communities
tend to be insular by nature.

Direct runners often cheat when executing their matching language SDKs.

This is despite Beam providing cross language abilities by it's portable nature.

The space is large and complicated, which we try to sort out with these large tables, showing
what capabilities SDKs have, versus what the runners are able to execute.

When an error occurs, in many runners, it's not entirely clear what the remeadiation strategy is.
We've built Beam runners on top of Flink and Spark, but is the error from my pipeline,
from the Beam interface layers, or from the underlying engine?

Dataflow has various logs, but one can sometimes need to be a Dataflow engineer to dig deeper into
an issue there.

## Runners as a piece of SDK development

Beam provides a model and set of interfaces for how to build and execute a data pipeline.

We provide a guide for how to author a runner, which is generally tuned to how to wrap an existing runner,
and points to wrappers authored in Java. It doesn't really teach how to write a runner.

We don't provide an SDK authoring Guide at all, and one either needs to already be a expert in the Beam model
or authored an existing SDK to be able to bootstrap the process.

One also needs to be already comfortable with starting up a runner on the side to
 execute against, if you manage to get far enough to submit jobs as portable beam pipelines.

## History

Development on Prism started in late 2022 as lostluck wanted an easier way to validate
Beam pipelines. In particular for the Go SDK.

It was too hard to use a runner that was not built into the SDK.

Each of the other available runners were not appropriate for this purpose, or required
heavier weight set up.

It's much easier these days to set up Python and Java, but tooling is inconsistent for
those not immersed in it.


## Why in Go?

Largely because Prism came out of an effort to make the Go SDK easier to test and adopt, it was a natural fit.

But Go has advantages over Java and Python when it comes to portable use.

Go compiles to small, static binaries.
Go can compile binaries cross platform trivially.
It's garbage collected, which meshes well with how Beam operates.

While the Beam Java and Python SDK Worker containers need hundreds to thousands of megabytes of code and jars.

