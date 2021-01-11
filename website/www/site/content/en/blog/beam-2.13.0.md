---
title:  "Apache Beam 2.13.0"
date:   2019-06-07 00:00:01 -0800
categories:
  - blog
# Date above corrected but keep the old URL:
aliases:
  - /blog/2019/05/22/beam-2.13.0.html
authors:
        - goenka

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

We are happy to present the new 2.13.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2130-2019-05-21) for this release.<!--more-->
For more information on changes in 2.13.0, check out the
[detailed release notes](https://jira.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12345166).

## Highlights

### I/Os

* Support reading query results with the BigQuery storage API.
* Support KafkaIO to be configured externally for use with other SDKs.
* BigQuery IO now supports BYTES datatype on Python 3.
* Avro IO support enabled on Python 3.
* For Python 3 pipelines, the default Avro library used by Beam AvroIO and Dataflow workers was switched from avro-python3 to fastavro.


### New Features / Improvements

* Flink 1.8 support added.
* Support to run word count on Portable Spark runner.
* ElementCount metrics in FnApi Dataflow Runner.
* Support to create BinaryCombineFn from lambdas.


### Breaking Changes
* When writing BYTES Datatype into Bigquery with Beam Bigquery IO on Python DirectRunner, users need to base64-encode bytes values before passing them to Bigquery IO. Accordingly, when reading bytes data from BigQuery, the IO will also return base64-encoded bytes. This change only affects Bigquery IO on Python DirectRunner. New DirectRunner behavior is consistent with treatment of Bytes by Beam Java Bigquery IO, and Python Dataflow Runner.


### Bugfixes

* Various bug fixes and performance improvements.

## List of Contributors

According to git shortlog, the following people contributed to the 2.13.0 release. Thank you to all contributors!

Aaron Li, Ahmet Altay, Aizhamal Nurmamat kyzy, Alex Amato, Alexey Romanenko,
Andrew Pilloud, Ankur Goenka, Anton Kedin, apstndb, Boyuan Zhang, Brian Hulette,
Brian Quinlan, Chamikara Jayalath, Cyrus Maden, Daniel Chen, Daniel Oliveira,
David Cavazos, David Moravek, David Yan, EdgarLGB, Etienne Chauchot, frederik2,
Gleb Kanterov, Harshit Dwivedi, Harsh Vardhan, Heejong Lee, Hennadiy Leontyev,
Henri-Mayeul de Benque, Ismaël Mejía, Jae-woo Kim, Jamie Kirkpatrick, Jan Lukavský,
Jason Kuster, Jean-Baptiste Onofré, JohnZZGithub, Jozef Vilcek, Juta, Kenneth Jung,
Kenneth Knowles, Kyle Weaver, Łukasz Gajowy, Luke Cwik, Mark Liu, Mathieu Blanchard,
Maximilian Michels, Melissa Pashniak, Michael Luckey, Michal Walenia, Mike Kaplinskiy,
Mike Pedersen, Mikhail Gryzykhin, Mikhail-Ivanov, Niklas Hansson, pabloem,
Pablo Estrada, Pranay Nanda, Reuven Lax, Richard Moorhead, Robbe Sneyders,
Robert Bradshaw, Robert Burke, Roman van der Krogt, rosetn, Rui Wang, Ryan Yuan,
Sam Whittle, sudhan499, Sylwester Kardziejonek, Ted, Thomas Weise, Tim Robertson,
ttanay, tvalentyn, Udi Meiri, Valentyn Tymofieiev, Xinyu Liu, Yifan Zou,
yoshiki.obata, Yueyang Qiu
