---
title:  "Go SDK Exits Experimental in Apache Beam 2.34.0"
date:   2021-11-05 00:00:01 -0800
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

Apache Beam’s latest release, version [2.34.0](/get-started/downloads/), is the first official release of the long experimental Go SDK.
Built with the [Go Programming Language](https://golang.org/), the Go SDK joins the Java and Python SDKs as the third implementation of the Beam programming model.

<!--more-->

## Using the new Go SDK.

New users of the Go SDK can start using it in their Go programs:

```
import "github.com/apache/beam/sdks/v2/go/pkg/beam"
```

The next run of `go mod tidy` will fetch the latest stable version.
Alternatively executing `go get github.com/apache/beam/sdks/v2/go/pkg/beam` will download it to the local module cache immeadiately, and add it to your `go.mod` file.

Existing users of the experimental Go SDK need to update to new `v2` import paths to start using the latest versions of the SDK.
This is can be done by adding `v2` to the import paths, changing `github.com/apache/beam/sdks/go/`... to `github.com/apache/beam/sdks/v2/go/`... where applicable, and then running `go mod tidy`.

Further documentation on using the SDK is available in the [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/), and in the package Go Doc at https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam.

## Feature Support

At time of writing, the Go SDK is currently "Batteries Not Included".
This means that there are gaps or edge cases in supported IOs and transforms.
That said, the core of the SDK enables a great deal of the Beam Model for
custom user use, supporting following features:

* PTransforms
  * Impulse
  * Create
  * ParDo w/User DoFns
    * Iterable Side Inputs
    * Multiple Output emitters
    * Receive and Return Key Value pairs
    * EventTime
    * SplittableDoFns
  * GroupByKey and CoGroupByKey
  * CombineFns for use with Combine and CombinePerKey
  * Windowing
    * Global, Fixed intervals, Sliding windows, and Session
    * Aggregating over windowed PCollections w/GBKs or Combines.
  * Flatten
  * Partition
  * Composites
  * Cross Language Transforms
* Coders
  * Primitive Go types (ints, string, []bytes, and more)
  * Beam Schemas for Go Struct types (including struct, slice, and map fields)
  * Registering custom coders
* Metrics
  * PCollection metrics (ElementCount, Size Estimates)
  * Custom User Metrics
  * DoFn profiling metrics (coming in 2.35.0)
* Provided Built Transforms
  * Sum, Count, Min, Max, Top, Filter
  * Scalable TextIO Reading
  * Test Stream (for runners that support it.)

Upcoming features support roadmap, and known issues are discussed below.

## Releases

With this release, the Go SDK now uses [Go Modules](https://golang.org/ref/mod) for dependency management.
This makes it so users, SDK Devs, and the testing infrastructure can all rely on the same versions of dependencies, making builds reproducible.

Versioned SDK worker containers are now built and [published](https://hub.docker.com/r/apache/beam_go_sdk/tags?page=1&ordering=last_updated), with the SDK using matched tagged versions.
For released versions, user jobs no longer need to specify a container to use, except when using custom containers.

## Compatibility

The Go SDK will largely follow suite with the Go notion of compatibility.
Some concessions are made to keep all SDKs together on the same release cycle.

### Language Compatibility

The SDK will be tested at a minimum [Go Programming Language version of 1.16](https://golang.org/doc/devel/release), and use available language features and standard library packages accordingly.
To maintain a broad compatibility, the Go SDK will not require the latest major version of Go.
We expect to follow the 2nd newest supported release of the language, with a possible exception when Go 1.18 is released, in order to begin experimenting with [Go Generics](https://go.dev/blog/generics-proposal) in the SDK.
Release notes will call out when the minimum version of the language changes.

### Package Compatibility

The primary user packages will avoid changing in backwards incompatible ways for core features.
This is to be inline with Go's notion of the [`import compatibility rule`](https://research.swtch.com/vgo-import).

> If an old package and a new package have the same import path,
> the new package must be backwards compatible with the old package.

Exceptions to this policy are around newer, experimental, or in development features.
These are subject to change.
Such features will have a doc comment noting the experimental status, or be mentioned the change notes for a release.
For example, using `beam.WindowInto` with Triggers is currently experimental and may have the api changed in a future release.

Primary user packages include:
* The main beam package `github.com/apache/beam/sdks/v2/go/pkg/beam`
* Sub packages under `.../transforms`, `.../io`, `.../runners`, and `.../testing`.

Generally, packages in the module other than the primary user packages are for framework use.
They are at risk of changing, though at this point, large breaking changes are unlikely.

### Known Issues

DO NOT SUBMIT: include Known Issues w/ 2.33.0, and known features making 2.34.0 and 2.35.0  (side inputs, metrics queyrying, IOs, Direct Runner, Triggers, Test Stream,  Cross Language Transforms caveats)

* Batteries not included.
    * Current native transforms are undertested.
    * IOs may not be written to scale.
    * Need something?
      * File a ticket in the [Beam JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20sdk-go) and,
      * Email the dev@beam.apache.org list!
* Non-Global Window Side Inputs don't match (fixed in 2.35.0)
* Side Inputs accumulate memory over large element count bundles (fixed in 2.35.0)
* Go Direct Runner is incomplete and is not portable.
* Trigger API is under iteration and subject to change.
* Support of the SDK on services, like Google Cloud Dataflow, remains at the service owner's discretion.

## Roadmap

The [SDK roadmap](https://beam.apache.org/roadmap/go-sdk/) has been updated.
Ongoing focus is to bolster more streaming focused features, and improve existing connectors, and make connectors easier to implement.

In the nearer term this comes in the form of improvements to Side Inputs, and providing wrappers and improving ease-of-use for Cross Language Transforms from Java.

## Conclusion

I hope you find the SDK useful, and it's still early days.
If you make something using it, consider [sharing it with us](https://beam.apache.org/community/contact-us/).

And remember, [contributions](https://beam.apache.org/contribute/) are always welcome.




