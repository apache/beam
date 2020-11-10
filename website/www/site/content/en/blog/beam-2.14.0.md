---
title:  "Apache Beam 2.14.0"
date:   2019-07-31 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2019/07/31/beam-2.14.0.html
authors:
        - anton
        - altay

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

We are happy to present the new 2.14.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2140-2019-08-01) for this release.<!--more-->
For more information on changes in 2.14.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12345431).

## Highlights

 * Python 3 support is extended to Python 3.6 and 3.7; in addition to various other Python 3 [improvements](https://issues.apache.org/jira/browse/BEAM-1251?focusedCommentId=16890504&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-16890504).
 * Spark portable runner (batch) now [available](https://lists.apache.org/thread.html/c43678fc24c9a1dc9f48c51c51950aedcb9bc0fd3b633df16c3d595a@%3Cuser.beam.apache.org%3E) for Java, Python, Go.
 * Added new runner: Hazelcast Jet Runner. ([BEAM-7305](https://issues.apache.org/jira/browse/BEAM-7305))

### I/Os

* Schema support added to BigQuery reads. (Java) ([BEAM-6673](https://issues.apache.org/jira/browse/BEAM-6673))
* Schema support added to JDBC source. (Java) ([BEAM-6674](https://issues.apache.org/jira/browse/BEAM-6674))
* BigQuery support for `bytes` is fixed. (Python 3) ([BEAM-6769](https://issues.apache.org/jira/browse/BEAM-6769))
* Added DynamoDB IO. (Java) ([BEAM-7043](https://issues.apache.org/jira/browse/BEAM-7043))
* Added support unbounded reads with HCatalogIO (Java) ([BEAM-7450](https://issues.apache.org/jira/browse/BEAM-7450))
* Added BoundedSource wrapper for SDF. (Python) ([BEAM-7443](https://issues.apache.org/jira/browse/BEAM-7443))
* Added support for INCRBY/DECRBY operations in RedisIO. ([BEAM-7286](https://issues.apache.org/jira/browse/BEAM-7286))
* Added Support for ValueProvider defined GCS Location for WriteToBigQuery with File Loads. (Java) (([BEAM-7603](https://issues.apache.org/jira/browse/BEAM-7603)))


### New Features / Improvements

* Python SDK add support for DoFn `setup` and `teardown` methods. ([BEAM-562](https://issues.apache.org/jira/browse/BEAM-562))
* Python SDK adds new transforms: [ApproximateUnique](https://issues.apache.org/jira/browse/BEAM-6693), [Latest](https://issues.apache.org/jira/browse/BEAM-6695), [Reify](https://issues.apache.org/jira/browse/BEAM-7019), [ToString](https://issues.apache.org/jira/browse/BEAM-7021), [WithKeys](https://issues.apache.org/jira/browse/BEAM-7023).
* Added hook for user-defined JVM initialization in workers. ([BEAM-6872](https://issues.apache.org/jira/browse/BEAM-6872))
* Added support for SQL Row Estimation for BigQueryTable. ([BEAM-7513](https://issues.apache.org/jira/browse/BEAM-7513))
* Auto sharding of streaming sinks in FlinkRunner. ([BEAM-5865](https://issues.apache.org/jira/browse/BEAM-5865))
* Removed the Hadoop dependency from the external sorter. ([BEAM-7268](https://issues.apache.org/jira/browse/BEAM-7268))
* Added option to expire portable SDK worker environments. ([BEAM-7348](https://issues.apache.org/jira/browse/BEAM-7348))
* Beam does not relocate Guava anymore and depends only on its own vendored version of Guava. ([BEAM-6620](https://issues.apache.org/jira/browse/BEAM-6620))


### Breaking Changes
* Deprecated set/getClientConfiguration in Jdbc IO. ([BEAM-7263](https://issues.apache.org/jira/browse/BEAM-7263))


### Bugfixes

* Fixed reading of concatenated compressed files. (Python) ([BEAM-6952](https://issues.apache.org/jira/browse/BEAM-6952))
* Fixed re-scaling issues on Flink >= 1.6 versions. ([BEAM-7144](https://issues.apache.org/jira/browse/BEAM-7144))
* Fixed SQL EXCEPT DISTINCT behavior. ([BEAM-7194](https://issues.apache.org/jira/browse/BEAM-7194))
* Fixed OOM issues with bounded Reads for Flink Runner. ([BEAM-7442](https://issues.apache.org/jira/browse/BEAM-7442))
* Fixed HdfsFileSystem to correctly match directories. ([BEAM-7561](https://issues.apache.org/jira/browse/BEAM-7561))
* Upgraded Spark runner to use spark version 2.4.3. ([BEAM-7265](https://issues.apache.org/jira/browse/BEAM-7265))
* Upgraded Jackson to version 2.9.9. ([BEAM-7465](https://issues.apache.org/jira/browse/BEAM-7465))
* Various other bug fixes and performance improvements.


### Known Issues

* Do **NOT** use Python MongoDB source in this release. Python MongoDB source [added](https://issues.apache.org/jira/browse/BEAM-5148) in this release has a known issue that can result in data loss. See ([BEAM-7866](https://issues.apache.org/jira/browse/BEAM-7866)) for details.
* Can't install the Python SDK on macOS 10.15. See ([BEAM-8368](https://issues.apache.org/jira/browse/BEAM-8368)) for details.


## List of Contributors

According to git shortlog, the following people contributed to the 2.14.0 release. Thank you to all contributors!

Ahmet Altay, Aizhamal Nurmamat kyzy, Ajo Thomas, Alex Amato, Alexey Romanenko,
Alexey Strokach, Alex Van Boxel, Alireza Samadian, Andrew Pilloud,
Ankit Jhalaria, Ankur Goenka, Anton Kedin, Aryan Naraghi, Bartok Jozsef,
Bora Kaplan, Boyuan Zhang, Brian Hulette, Cam Mach, Chamikara Jayalath,
Charith Ellawala, Charles Chen, Colm O hEigeartaigh, Cyrus Maden,
Daniel Mills, Daniel Oliveira, David Cavazos, David Moravek, David Yan,
Daniel Lescohier, Elwin Arens, Etienne Chauchot, Fábio Franco Uechi,
Finch Keung, Frederik Bode, Gregory Kovelman, Graham Polley, Hai Lu, Hannah Jiang,
Harshit Dwivedi, Harsh Vardhan, Heejong Lee, Henry Suryawirawan,
Ismaël Mejía, Jan Lukavský, Jean-Baptiste Onofré, Jozef Vilcek, Juta, Kai Jiang,
Kamil Wu, Kasia Kucharczyk, Kenneth Knowles, Kyle Weaver, Lara Schmidt,
Łukasz Gajowy, Luke Cwik, Manu Zhang, Mark Liu, Matthias Baetens,
Maximilian Michels, Melissa Pashniak, Michael Luckey, Michal Walenia,
Mikhail Gryzykhin, Ming Liang, Neville Li, Pablo Estrada, Paul Suganthan,
Peter Backx, Rakesh Kumar, Rasmi Elasmar, Reuven Lax, Reza Rokni, Robbe Sneyders,
Robert Bradshaw, Robert Burke, Rose Nguyen, Rui Wang, Ruoyun Huang,
Shoaib Zafar, Slava Chernyak, Steve Niemitz, Tanay Tummalapalli, Thomas Weise,
Tim Robertson, Tim van der Lippe, Udi Meiri, Valentyn Tymofieiev, Varun Dhussa,
Viktor Gerdin, Yichi Zhang, Yifan Mai, Yifan Zou, Yueyang Qiu.
