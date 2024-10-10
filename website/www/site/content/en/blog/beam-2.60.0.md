---
title:  "Apache Beam 2.60.0"
date:   2024-10-XX 13:00:00 -0800
categories:
  - blog
  - release
authors:
  - yhu
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

We are happy to present the new 2.60.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2600-2024-10-XX) for this release.

<!--more-->

For more information on changes in 2.60.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/24).

## Highlights

* Added support for using vLLM in the RunInference transform (Python) ([#32528](https://github.com/apache/beam/issues/32528))
* [Managed Iceberg] Added support for streaming writes ([#32451](https://github.com/apache/beam/pull/32451))
* [Managed Iceberg] Added auto-sharding for streaming writes ([#32612](https://github.com/apache/beam/pull/32612))
* [Managed Iceberg] Added support for writing to dynamic destinations ([#32565](https://github.com/apache/beam/pull/32565))

## New Features / Improvements

* Dataflow worker can install packages from Google Artifact Registry Python repositories (Python) ([#32123](https://github.com/apache/beam/issues/32123)).
* Added support for Zstd codec in SerializableAvroCodecFactory (Java) ([#32349](https://github.com/apache/beam/issues/32349))
* Added support for using vLLM in the RunInference transform (Python) ([#32528](https://github.com/apache/beam/issues/32528))
* Prism release binaries and container bootloaders are now being built with the latest Go 1.23 patch. ([#32575](https://github.com/apache/beam/pull/32575))
* Prism
  * Prism now supports Bundle Finalization. ([#32425](https://github.com/apache/beam/pull/32425))
* Significantly improved performance of Kafka IO reads that enable [commitOffsetsInFinalize](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/kafka/KafkaIO.Read.html#commitOffsetsInFinalize--) by removing the data reshuffle from SDF implementation.  ([#31682](https://github.com/apache/beam/pull/31682)).
* Added support for dynamic writing in MqttIO (Java) ([#19376](https://github.com/apache/beam/issues/19376))
* Optimized Spark Runner parDo transform evaluator (Java) ([#32537](https://github.com/apache/beam/issues/32537))
* [Managed Iceberg] More efficient manifest file writes/commits ([#32666](https://github.com/apache/beam/issues/32666))

## Breaking Changes

* In Python, assert_that now throws if it is not in a pipeline context instead of silently succeeding ([#30771](https://github.com/apache/beam/pull/30771))
* In Python and YAML, ReadFromJson now override the dtype from None to
  an explicit False.  Most notably, string values like `"123"` are preserved
  as strings rather than silently coerced (and possibly truncated) to numeric
  values.  To retain the old behavior, pass `dtype=True` (or any other value
  accepted by `pandas.read_json`).
* Users of KafkaIO  Read transform that enable [commitOffsetsInFinalize](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/kafka/KafkaIO.Read.html#commitOffsetsInFinalize--) might encounter pipeline graph  compatibility issues when updating the pipeline. To mitigate, set the `updateCompatibilityVersion` option to the SDK version used for the original pipeline, example `--updateCompatabilityVersion=2.58.1`

## Deprecations

* Python 3.8 is reaching EOL and support is being removed in Beam 2.61.0. The 2.60.0 release will warn users
when running on 3.8. ([#31192](https://github.com/apache/beam/issues/31192))

## Bugfixes

* (Java) Fixed custom delimiter issues in TextIO ([#32249](https://github.com/apache/beam/issues/32249), [#32251](https://github.com/apache/beam/issues/32251)).
* (Java, Python, Go) Fixed PeriodicSequence backlog bytes reporting, which was preventing Dataflow Runner autoscaling from functioning properly ([#32506](https://github.com/apache/beam/issues/32506)).
* (Java) Fix improper decoding of rows with schemas containing nullable fields when encoded with a schema with equal encoding positions but modified field order. ([#32388](https://github.com/apache/beam/issues/32388)).

## Known Issues

N/A

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.59.0 release. Thank you to all contributors!

Ahmed Abualsaud, Aiden Grossman, Arun Pandian, Bartosz Zablocki, Chamikara Jayalath, Claire McGinty, DKPHUONG, Damon Douglass, Danny McCormick, Dip Patel, Ferran Fernández Garrido, Hai Joey Tran, Hyeonho Kim, Igor Bernstein, Israel Herraiz, Jack McCluskey, Jaehyeon Kim, Jeff Kinard, Jeffrey Kinard, Joey Tran, Kenneth Knowles, Kirill Berezin, Michel Davit, Minbo Bae, Naireen Hussain, Niel Markwick, Nito Buendia, Reeba Qureshi, Reuven Lax, Robert Bradshaw, Robert Burke, Rohit Sinha, Ryan Fu, Sam Whittle, Shunping Huang, Svetak Sundhar, Udaya Chathuranga, Vitaly Terentyev, Vlado Djerek, Yi Hu, Claude van der Merwe, XQ Hu, Martin Trieu, Valentyn Tymofieiev, twosom
