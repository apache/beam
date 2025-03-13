---
title:  "Apache Beam 2.58.1"
date:   2024-08-15 13:00:00 -0800
categories:
  - blog
  - release
authors:
  - damccorm
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

We are happy to present the new 2.58.1 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2580-2024-08-06) for this release.

<!--more-->

## New Features / Improvements

* Fixed issue where KafkaIO Records read with `ReadFromKafkaViaSDF` are redistributed and may contain duplicates regardless of the configuration. This affects Java pipelines with Dataflow v2 runner and xlang pipelines reading from Kafka, ([#32196](https://github.com/apache/beam/issues/32196))

## Known Issues

* Large Dataflow graphs using runner v2, or pipelines explicitly enabling the `upload_graph` experiment, will fail at construction time ([#32159](https://github.com/apache/beam/issues/32159)).
* Python pipelines that run with 2.53.0-2.58.0 SDKs and read data from GCS might be affected by a data corruption issue ([#32169](https://github.com/apache/beam/issues/32169)). The issue will be fixed in 2.59.0 ([#32135](https://github.com/apache/beam/pull/32135)). To work around this, update the google-cloud-storage package to version 2.18.2 or newer.

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.58.1 release. Thank you to all contributors!

Danny McCormick

Sam Whittle
