---
title:  "Apache Beam 2.55.0"
date:   2024-03-25 10:00:00 -0400
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

We are happy to present the new 2.55.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2550-2023-03-25) for this release.

<!--more-->

For more information on changes in 2.55.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/19).

## Highlights

* The Python SDK will now include automatically generated wrappers for external Java transforms! ([#29834](https://github.com/apache/beam/pull/29834))

## I/Os

* Added support for handling bad records to BigQueryIO ([#30081](https://github.com/apache/beam/pull/30081)).
  * Full Support for Storage Read and Write APIs
  * Partial Support for File Loads (Failures writing to files supported, failures loading files to BQ unsupported)
  * No Support for Extract or Streaming Inserts
* Added support for handling bad records to PubSubIO ([#30372](https://github.com/apache/beam/pull/30372)).
  * Support is not available for handling schema mismatches, and enabling error handling for writing to Pub/Sub topics with schemas is not recommended
* `--enableBundling` pipeline option for BigQueryIO DIRECT_READ is replaced by `--enableStorageReadApiV2`. Both were considered experimental and subject to change (Java) ([#26354](https://github.com/apache/beam/issues/26354)).

## New Features / Improvements

* Allow writing clustered and not time-partitioned BigQuery tables (Java) ([#30094](https://github.com/apache/beam/pull/30094)).
* Redis cache support added to RequestResponseIO and Enrichment transform (Python) ([#30307](https://github.com/apache/beam/pull/30307))
* Merged `sdks/java/fn-execution` and `runners/core-construction-java` into the main SDK. These artifacts were never meant for users, but noting
  that they no longer exist. These are steps to bring portability into the core SDK alongside all other core functionality.
* Added Vertex AI Feature Store handler for Enrichment transform (Python) ([#30388](https://github.com/apache/beam/pull/30388))

## Breaking Changes

* Arrow version was bumped to 15.0.0 from 5.0.0 ([#30181](https://github.com/apache/beam/pull/30181)).
* Go SDK users who build custom worker containers may run into issues with the move to distroless containers as a base (see Security Fixes).
  * The issue stems from distroless containers lacking additional tools, which current custom container processes may rely on.
  * See https://beam.apache.org/documentation/runtime/environments/#from-scratch-go for instructions on building and using a custom container.
* Python SDK has changed the default value for the `--max_cache_memory_usage_mb` pipeline option from 100 to 0. This option was first introduced in the 2.52.0 SDK version. This change restores the behavior of the 2.51.0 SDK, which does not use the state cache. If your pipeline uses iterable side inputs views, consider increasing the cache size by setting the option manually. ([#30360](https://github.com/apache/beam/issues/30360)).

## Deprecations

* N/A

## Bug fixes

* Fixed `SpannerIO.readChangeStream` to support propagating credentials from pipeline options
  to the `getDialect` calls for authenticating with Spanner (Java) ([#30361](https://github.com/apache/beam/pull/30361)).
* Reduced the number of HTTP requests in GCSIO function calls (Python) ([#30205](https://github.com/apache/beam/pull/30205))

## Security Fixes

* Go SDK base container image moved to distroless/base-nossl-debian12, reducing vulnerable container surface to kernel and glibc ([#30011](https://github.com/apache/beam/pull/30011)).

## Known Issues

* In Python pipelines, when shutting down inactive bundle processors, shutdown logic can overaggressively hold the lock, blocking acceptance of new work. Symptoms of this issue include slowness or stuckness in long-running jobs. Fixed in 2.56.0 ([#30679](https://github.com/apache/beam/pull/30679)).
* Python pipelines that run with 2.53.0-2.58.0 SDKs and read data from GCS might be affected by a data corruption issue ([#32169](https://github.com/apache/beam/issues/32169)). The issue will be fixed in 2.59.0 ([#32135](https://github.com/apache/beam/pull/32135)). To work around this, update the google-cloud-storage package to version 2.18.2 or newer.

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.55.0 release. Thank you to all contributors!

Ahmed Abualsaud

Anand Inguva

Andrew Crites

Andrey Devyatkin

Arun Pandian

Arvind Ram

Chamikara Jayalath

Chris Gray

Claire McGinty

Damon Douglas

Dan Ellis

Danny McCormick

Daria Bezkorovaina

Dima I

Edward Cui

Ferran Fernández Garrido

GStravinsky

Jan Lukavský

Jason Mitchell

JayajP

Jeff Kinard

Jeffrey Kinard

Kenneth Knowles

Mattie Fu

Michel Davit

Oleh Borysevych

Ritesh Ghorse

Ritesh Tarway

Robert Bradshaw

Robert Burke

Sam Whittle

Scott Strong

Shunping Huang

Steven van Rossum

Svetak Sundhar

Talat UYARER

Ukjae Jeong (Jay)

Vitaly Terentyev

Vlado Djerek

Yi Hu

akashorabek

case-k

clmccart

dengwe1

dhruvdua

hardshah

johnjcasey

liferoad

martin trieu

tvalentyn
