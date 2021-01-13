---
title:  "Apache Beam 2.3.0"
date:   2018-02-19 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2018/02/19/beam-2.3.0.html
authors:
  - iemejia
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

We are glad to present the new 2.3.0 release of Beam. This release includes
multiple fixes and new functionalities. <!--more--> For more information
please check the detailed release notes.

# New Features / Improvements

## Beam moves to Java 8

The supported version of Java for Beam is now Java 8. The code and examples have
been refactored to use multiple of the advantages of the language, e.g. lambdas,
streams, improved type inference, etc.

## Spark runner is now based on Spark 2.x

Spark runner moves forward into the Spark 2.x development line, this would allow
to benefit of improved performance, as well as open the runner for future
compatibility with the Structured Streaming APIs. Notice that support for Spark
1.x is finished with this release.

## Amazon Web Services S3 Filesystem support

Beam already supported AWS S3 via HadoopFileSystem, but this version brings a
native implementation with the corresponding performance advantages of the S3
filesystem.

## General-purpose writing to files

This release contains a new transform, FileIO.write() / writeDynamic() that
implements a general-purpose fluent and Java8-friendly API for writing to files
using a FileIO.Sink. This API has similar capabilities to DynamicDestinations
APIs from Beam 2.2 but is much easier to use and extend. The DynamicDestinations
APIs for writing to files are deprecated by it, as is FileBasedSink.

## Splittable DoFn support on the Python SDK

This release adds the Splittable DoFn API for Python SDK and adds Splittable
DoFn support for Python streaming DirectRunner.

## Portability

Progress continues to being able to execute Python on runners other then Google
Cloud Dataflow and the Go SDK on any runner.

# Miscellaneous Fixes

## SDKs

* MapElements and FlatMapElements support using side inputs using the new
  interface Contextful.Fn. For library authors, this interface is the
  recommended choice for user-code callbacks that may use side inputs.
* Introduces the family of Reify transforms for converting between explicit and
  implicit representations of various Beam entities.
* Introduces two transforms for approximate sketching of data: Count-Min Sketch
  (approximate element frequency estimation) and HyperLogLog (approximate
  cardinality estimation).

## Runners

* Staging files on Dataflow shows progress
* Flink runner is based now on Flink version 1.4.0

## IOs

* BigtableIO now supports ValueProvider configuration
* BigQueryIO supports writing bounded collections to tables with partition
  decorators
* KafkaIO moves to version 1.0 (it is still backwards compatible with versions >= 0.9.x.x)
* Added IO source for VCF files (Python)
* Added support for backoff on deadlocks in JdbcIO.write() and connection
  improvement
* Improved performance of KinesisIO.read()
* Many improvements to TikaIO

# List of Contributors

According to git shortlog, the following 78 people contributed to the 2.3.0 release. Thank you to all contributors!

Ahmet Altay, Alan Myrvold, Alex Amato, Alexey Romanenko, Ankur Goenka, Anton Kedin, Arnaud Fournier, Asha Rostamianfar, Ben Chambers, Ben Sidhom, Bill Neubauer, Brian Foo, cclauss, Chamikara Jayalath, Charles Chen, Colm O hEigeartaigh, Daniel Oliveira, Dariusz Aniszewski, David Cavazos, David Sabater, David Sabater Dinter, Dawid Wysakowicz, Dmytro Ivanov, Etienne Chauchot, Eugene Kirpichov, Exprosed, Grzegorz Kołakowski, Henning Rohde, Holden Karau, Huygaa Batsaikhan, Ilya Figotin, Innocent Djiofack, Ismaël Mejía, Itamar Ostricher, Jacky, Jacob Marble, James Xu, Jean-Baptiste Onofré, Jeremie Lenfant-Engelmann, Kamil Szewczyk, Kenneth Knowles, Lukasz Cwik, Łukasz Gajowy, Luke Zhu, Mairbek Khadikov, María García Herrero, Marian Dvorsky, Mark Liu, melissa, Miles Saul, mingmxu, Motty Gruda, nerdynick, Neville Li, Nigel Kilmer, Pablo, Pawel Kaczmarczyk, Petr Shevtsov, Rafal Wojdyla, Raghu Angadi, Robert Bradshaw, Robert Burke, Romain Manni-Bucau, Ryan Niemocienski, Ryan Skraba, Sam Whittle, Scott Wegner, Shashank Prabhakara, Solomon Duskis, Thomas Groh, Thomas Weise, Udi Meiri, Valentyn Tymofieiev, wtanaka.com, XuMingmin, zhouhai02, Zohar Yahav, 琨瑜.

