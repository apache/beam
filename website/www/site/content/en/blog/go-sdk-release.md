---
title:  "Go SDK Exits Experimental in Apache Beam 2.33.0"
date:   2021-11-02 00:00:01 -0800
categories:
  - blog
authors:
  - lostluck
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

Apache Beam’s latest release, version [2.33.0](/get-started/downloads/), is the first official release of the long experimental Go SDK.
Built with the [Go Programming Language](https://golang.org/), the Go SDK joins the Java and Python SDKs as the third implementation of the Beam programming model.

<!--more-->

The Go SDK

## tl;dr;

At time of writing, the Go SDK is currently "Batteries Not Included".
This means that there are gaps or edge cases in supported IOs and transforms. 
That said, the core of the SDK enables a great deal of the Beam Model for
custom user use, supporting following features:

* PTransforms:
  * User DoFns
    * Side Inputs
    * Multiple Outputs
    * SplittableDoFns
  * GroupByKey and CoGroupByKey
  * Windowing
    * Global, Fixed intervals, Sliding windows, and Session
  * CombineFns for use with Combine and CombinePerKey
  * Flatten
  * Partition
  * Composites
  * Cross Language Transforms
* Built-in Coders for: 
  * Primitive Go types (ints, string, []bytes)
  * Beam Schemas for Go Struct types (including struct, slice, and map fields)
* Metrics
  * PCollection metrics (ElementCount, Size Estimates)
  * Custom User Metrics
  
DO NOT SUBMIT: Finish the Litany of features.

## Releases

With this release, the Go SDK now uses [Go Modules](https://golang.org/ref/mod) for dependency management.
This makes it so users, SDK Devs, and the testing infrastructure can all rely on the same versions of dependencies, making builds reproducible.

Versioned SDK worker containers are now built and [published](https://hub.docker.com/r/apache/beam_go_sdk/tags?page=1&ordering=last_updated), with the SDK using matched tagged versions.
For released versions, user jobs no longer need to specify a container to use, except when using custom containers.

## Compatibility


DO NOT SUBMIT: Include Compatibility Statement WRT Experimental features.
* Triggers call out
* Cross Language transforms

DO NOT SUBMIT: include Known Issues w/ 2.33.0, and known features making 2.34.0 and 2.35.0  (side inputs, metrics queyrying, IOs, Direct Runner, Triggers, Test Stream,  Cross Language Transforms caveats)


DO NOT SUBMIT: Call out roadmap, Generics, Streaming Features (link to Streaming plan, Checkpoints, State and Timers) etc


## The story of the Go SDK

TODO Short History of the Go SDK, Why It's taken so long (Portability Stability), etc.

* The First Portability first SDK.
    * Can only run on Portable Runners, like Flink, Spark, Samza, and Cloud Dataflow.
* New SDKs should take less time.

TODO Addendum: Write an opinionated guide to writing an SDK. (Coders, Pipeline abstraction, CrossLanguage)

# Known Issues

* Batteries not included.
    * Current native transforms are likely undertested, and may not scale.
    * However, users are able to write their own Splittable DoFns as a work around.
* Non-Global Window Side Inputs don't match (fixed in 2.35.0)
* Side Inputs accumulate memory over large element count bundles (fixed in 2.35.0)
* 
* Go Direct Runner is incomplete and is not portable.


