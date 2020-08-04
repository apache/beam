---
title:  "Apache Beam 2.20.0"
date:   2020-04-15 00:00:01 -0800
# Date above corrected but keep the old URL:
aliases: 
  - /blog/2020/03/01/beam-2.20.0.html
  - /blog/2020/04/15/beam-2.20.0.html
categories: 
  - blog
authors:
  - amaliujia
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

We are happy to present the new 2.20.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2190-2020-02-04) for this release.<!--more-->
For more information on changes in 2.20.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12346780).

### I/Os
Python SDK: . (#10223).
* [BEAM-8561](https://issues.apache.org/jira/browse/BEAM-8561) Adds support for Thrift encoded data via ThriftIO
* [BEAM-7310](https://issues.apache.org/jira/browse/BEAM-7310) KafkaIO supports schema resolution using Confluent Schema Registry
* [BEAM-7246](https://issues.apache.org/jira/browse/BEAM-7246) Support for Google Cloud Spanner. This is an experimental module for reading and writing data from Google Cloud Spanner
* [BEAM-8399](https://issues.apache.org/jira/browse/BEAM-8399) Adds support for standard HDFS URLs (with server name)

### New Features / Improvements
* [BEAM-9146](https://issues.apache.org/jira/browse/BEAM-9146) New AnnotateVideo & AnnotateVideoWithContext PTransform's that integrates GCP Video Intelligence functionality
* [BEAM-9247](https://issues.apache.org/jira/browse/BEAM-9247) New AnnotateImage & AnnotateImageWithContext PTransform's for element-wise & batch image annotation using Google Cloud Vision API
* [BEAM-9258](https://issues.apache.org/jira/browse/BEAM-9258) Added a PTransform for inspection and deidentification of text using Google Cloud DLP
* [BEAM-9248](https://issues.apache.org/jira/browse/BEAM-9248) New AnnotateText PTransform that integrates Google Cloud Natural Language functionality
* [BEAM-9305](https://issues.apache.org/jira/browse/BEAM-9305) ReadFromBigQuery now supports value providers for the query string
* [BEAM-8841](https://issues.apache.org/jira/browse/BEAM-8841) Added ability to write to BigQuery via Avro file loads
* [BEAM-9228](https://issues.apache.org/jira/browse/BEAM-9228) Direct runner for FnApi supports further parallelism
* [BEAM-8550](https://issues.apache.org/jira/browse/BEAM-8550) Support for @RequiresTimeSortedInput in Flink and Spark
* [BEAM-6857](https://issues.apache.org/jira/browse/BEAM-6857) Added support for dynamic timers

### Breaking Changes
* [BEAM-3453](https://issues.apache.org/jira/browse/BEAM-3453) Backwards incompatible change in ReadFromPubSub(topic=) in Python
* [BEAM-9310](https://issues.apache.org/jira/browse/BEAM-9310) SpannerAccessor in Java is now package-private to reduce API surface
* [BEAM-8616](https://issues.apache.org/jira/browse/BEAM-8616) ParquetIO hadoop dependency should be now provided by the users
* [BEAM-9063](https://issues.apache.org/jira/browse/BEAM-9063) Docker images will be deployed to apache/beam repositories from 2.20

### Bugfixes
* [BEAM-9579](https://issues.apache.org/jira/browse/BEAM-9579) Fixed numpy operators in ApproximateQuantiles
* [BEAM-9277](https://issues.apache.org/jira/browse/BEAM-9277) Fixed exception when running in IPython notebook
* [BEAM-1833](https://issues.apache.org/jira/browse/BEAM-1833) Restructure Python pipeline construction to better follow the Runner API
* [BEAM-9225](https://issues.apache.org/jira/browse/BEAM-9225) Fixed Flink uberjar job termination bug
* [BEAM-9503](https://issues.apache.org/jira/browse/BEAM-9503) Fixed SyntaxError in process worker startup
* Various bug fixes and performance improvements.

### Known Issues
* [BEAM-9322](https://issues.apache.org/jira/browse/BEAM-9322) Python SDK ignores manually set PCollection tags
* [BEAM-9445](https://issues.apache.org/jira/browse/BEAM-9445) Python SDK pre_optimize=all experiment may cause error
* [BEAM-9725](https://issues.apache.org/jira/browse/BEAM-9725) Python SDK performance regression for reshuffle transform

## List of Contributors

According to git shortlog, the following people contributed to the 2.20.0 release. Thank you to all contributors!

Ahmet Altay, Alex Amato, Alexey Romanenko, Andrew Pilloud, Ankur Goenka, Anton Kedin, Boyuan Zhang, Brian Hulette, Brian Martin, Chamikara Jayalath
, Charles Chen, Craig Chambers, Daniel Oliveira, David Moravek, David Rieber, Dustin Rhodes, Etienne Chauchot, Gleb Kanterov, Hai Lu, Heejong Lee
, Ismaël Mejía, J Ross Thomson, Jan Lukavský, Jason Kuster, Jean-Baptiste Onofré, Jeff Klukas, João Cabrita, Juan Rael, Juta, Kasia Kucharczyk
, Kengo Seki, Kenneth Jung, Kenneth Knowles, Kyle Weaver, Kyle Winkelman, Lukas Drbal, Marek Simunek, Mark Liu, Maximilian Michels, Melissa Pashniak
, Michael Luckey, Michal Walenia, Mike Pedersen, Mikhail Gryzykhin, Niel Markwick, Pablo Estrada, Pascal Gula, Rehman Murad Ali, Reuven Lax, Rob, Robbe Sneyders
, Robert Bradshaw, Robert Burke, Rui Wang, Ruoyun Huang, Ryan Williams, Sam Rohde, Sam Whittle, Scott Wegner, Shoaib Zafar, Thomas Weise, Tianyang Hu, Tyler Akidau
, Udi Meiri, Valentyn Tymofieiev, Xinyu Liu, XuMingmin, ttanay, tvalentyn, Łukasz Gajowy
