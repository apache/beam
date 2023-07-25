---
title:  "Go SDK Exits Experimental in Apache Beam 2.33.0"
date:   2021-11-04 00:00:01 -0800
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

## Using the new Go SDK.

New users of the Go SDK can start using it in their Go programs by importing the main beam package:

```
import "github.com/apache/beam/sdks/v2/go/pkg/beam"
```

The next run of `go mod tidy` will fetch the latest stable version of the module.
Alternatively executing `go get github.com/apache/beam/sdks/v2/go/pkg/beam` will download it to the local module cache immeadiately, and add it to your `go.mod` file.

Existing users of the experimental Go SDK need to update to new `v2` import paths to start using the latest versions of the SDK.
This can be done by adding `v2` to the import paths, changing `github.com/apache/beam/sdks/go/`... to `github.com/apache/beam/sdks/v2/go/`... where applicable, and then running `go mod tidy`.

Further documentation on using the SDK is available in the [Beam Programming Guide](/documentation/programming-guide/), and in the package [Go Doc](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam).

## Feature Support

At time of writing, the Go SDK is currently "Batteries Not Included".
This means that there are gaps or edge cases in supported IOs and transforms.
That said, the core of the SDK enables a great deal of the Beam Model for
custom user use, supporting the following features:

* PTransforms
  * Impulse
  * Create
  * ParDo with user DoFns
    * Iterable side inputs
    * Multiple output emitters
    * Receive and return key-value pairs
    * SplittableDoFns
  * GroupByKey and CoGroupByKey
  * Combine and CombinePerKey with user CombineFns
  * Flatten
  * Partition
  * Composite transforms
  * Cross language transforms
* Event time windowing
  * Global, Interval, Sliding, and Session windows
  * Aggregating over windowed PCollections with GroupByKeys or Combines
* Coders
  * Primitive Go types (ints, string, []bytes, and more)
  * Beam Schemas for Go struct types (including struct, slice, and map fields)
  * Registering custom coders
* Metrics
  * PCollection metrics (element counts, size estimates)
  * Custom user metrics
  * Post job user metrics querying (coming in 2.34.0)
  * DoFn profiling metrics (coming in 2.35.0)
* Built-in transforms
  * Sum, count, min, max, top, filter
  * Scalable TextIO reading

Upcoming feature roadmap, and known issues are discussed below.
In particular, we plan to support a much richer set of IO connectors via Beam's cross-language capabilities.

## Releases

With this release, the Go SDK now uses [Go Modules](https://golang.org/ref/mod) for dependency management.
This makes it so users, SDK authors, and the testing infrastructure can all rely on the same versions of dependencies, making builds reproducible.
This also makes [validating Go SDK Release Candidates simple](/blog/validate-beam-release/#configuring-a-go-build-to-validate-a-beam-release-candidate).

Versioned SDK worker containers are now built and [published](https://hub.docker.com/r/apache/beam_go_sdk/tags?page=1&ordering=last_updated), with the SDK using matching tagged versions.
User jobs no longer need to specify a container to use, except when using custom containers.

## Compatibility

The Go SDK will largely follow suit with the Go notion of compatibility.
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

Exceptions to this policy are around newer, experimental, or in development features and are subject to change.
Such features will have a doc comment noting the experimental status.
Major changes will be mentioned in the release notes.
For example, using `beam.WindowInto` with Triggers is currently experimental and may have the API changed in a future release.

Primary user packages include:
* The main beam package `github.com/apache/beam/sdks/v2/go/pkg/beam`
* Sub packages under `.../transforms`, `.../io`, `.../runners`, and `.../testing`.

Generally, packages in the module other than the primary user packages are for framework use and are at risk of changing.

### Known Issues

#### Batteries not included.
* Current native transforms are undertested
* IOs may not be written to scale
* Go Direct Runner is incomplete and is not portable, prefer using the Python Portable runner, or Flink
  * Doesn't support side input windowing. [BEAM-13075](https://issues.apache.org/jira/browse/BEAM-13075)
  * Doesn't serialize data, making it unlikely to catch coder issues [BEAM-6372](https://issues.apache.org/jira/browse/BEAM-6372)
  * Can use other general improvements, and become portable [BEAM-11076](https://issues.apache.org/jira/browse/BEAM-11076)
* Current Trigger API is under iteration and subject to change [BEAM-3304](https://issues.apache.org/jira/browse/BEAM-3304)
  * API has a possible breaking change between 2.33.0 and 2.34.0, and may change again
* Support of the SDK on services, like Google Cloud Dataflow, remains at the service owner's discretion
* Need something?
  * File a ticket in the [Beam JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20sdk-go) and,
  * Email the [dev@beam.apache.org](mailto:dev@beam.apache.org?subject=%5BGo%20SDK%20Feature%5D) list!

#### Fixed in 2.34.0
  * `top.SmallestPerKey` was broken [BEAM-12946](https://issues.apache.org/jira/browse/BEAM-12946)
  * `beam.TryCrossLanguage` API didn't match non-Try version [BEAM-9918](https://issues.apache.org/jira/browse/BEAM-9918)
    * This is a breaking change if one was calling `beam.TryCrossLanguage`

#### Fixed in 2.35.0
  * Non-global window side inputs don't match (correctness bug) [BEAM-11087](https://issues.apache.org/jira/browse/BEAM-11087)
    * Until 2.35.0 it's not recommended to use side inputs that are not using the global window.
  * DoFns using side inputs accumulate memory over bundles, causing out of memory issues [BEAM-13130](https://issues.apache.org/jira/browse/BEAM-13130)

## Roadmap

The [SDK roadmap](/roadmap/go-sdk/) has been updated.
Ongoing focus is to bolster streaming focused features, improve existing connectors, and make connectors easier to implement.

In the nearer term this comes in the form of improvements to side inputs, and providing wrappers and improving ease-of-use for cross language transforms from Java.

## Conclusion

We hope you find the SDK useful, and it's still early days.
If you make something with the Go SDK, consider [sharing it with us](/community/contact-us/).
And remember, [contributions](/contribute/) are always welcome.
