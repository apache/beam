---
title:  "Apache Beam 2.40.0"
date:   2022-06-25 9:00:00 -0700
categories:
  - blog
  - release
authors:
  - pabloem
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

We are happy to present the new 2.40.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2400-2022-06-25) for this
release.

<!--more-->

For more information on changes in 2.40.0 check out the [detailed release notes](https://github.com/apache/beam/releases/tag/v2.40.0).

## Highlights

* Added [RunInference](https://s.apache.org/inference-sklearn-pytorch) API, a framework agnostic transform for inference. With this release, PyTorch and Scikit-learn are supported by the transform.
    See also example at apache_beam/examples/inference/pytorch_image_classification.py

## I/Os

* Upgraded to Hive 3.1.3 for HCatalogIO. Users can still provide their own version of Hive. (Java) ([Issue-19554](https://github.com/apache/beam/issues/19554)).

## New Features / Improvements

* Go SDK users can now use generic registration functions to optimize their DoFn execution. ([BEAM-14347](https://issues.apache.org/jira/browse/BEAM-14347))
* Go SDK users may now write self-checkpointing Splittable DoFns to read from streaming sources. ([BEAM-11104](https://issues.apache.org/jira/browse/BEAM-11104))
* Go SDK textio Reads have been moved to Splittable DoFns exclusively. ([BEAM-14489](https://issues.apache.org/jira/browse/BEAM-14489))
* Pipeline drain support added for Go SDK has now been tested. ([BEAM-11106](https://issues.apache.org/jira/browse/BEAM-11106))
* Go SDK users can now see heap usage, sideinput cache stats, and active process bundle stats in Worker Status. ([BEAM-13829](https://issues.apache.org/jira/browse/BEAM-13829))
* The serialization (pickling)  library for Python is dill==0.3.1.1 ([BEAM-11167](https://issues.apache.org/jira/browse/BEAM-11167))

## Breaking Changes

* The Go Sdk now requires a minimum version of 1.18 in order to support generics ([BEAM-14347](https://issues.apache.org/jira/browse/BEAM-14347)).
* synthetic.SourceConfig field types have changed to int64 from int for better compatibility with Flink's use of Logical types in Schemas (Go) ([BEAM-14173](https://issues.apache.org/jira/browse/BEAM-14173))
* Default coder updated to compress sources used with `BoundedSourceAsSDFWrapperFn` and `UnboundedSourceAsSDFWrapper`.

## Bugfixes

* Fixed Java expansion service to allow specific files to stage ([BEAM-14160](https://issues.apache.org/jira/browse/BEAM-14160)).
* Fixed Elasticsearch connection when using both ssl and username/password (Java) ([BEAM-14000](https://issues.apache.org/jira/browse/BEAM-14000))

## Known Issues

* Python's ``beam.FlatMap`` will raise ``AttributeError:
  'builtin_function_or_method' object has no attribute '__func__'`` when
  constructed with some
  [built-ins](https://docs.python.org/3/library/functions.html), like ``sum``
  and ``len`` ([#22091](https://github.com/apache/beam/issues/22091)).
* Java's ``BigQueryIO.Write`` can have an exception where it attempts to output a timestamp beyond the max timestamp range
    ``Cannot output with timestamp 294247-01-10T04:00:54.776Z. Output timestamps must be no earlier than the timestamp of the current input or timer (294247-01-10T04:00:54.776Z) minus the allowed skew (0 milliseconds) and no later than 294247-01-10T04:00:54.775Z. See the DoFn#getAllowedTimestampSkew() Javadoc for details on changing the allowed skew.``
    This happens when a sink is idle, causing the idle timeout to trigger, or when a specific table is idle long enough when using dynamic destinations.
    When this happens, the job is no longer able to be drained. This has been fixed for the 2.41 release.

## List of Contributors

According to git shortlog, the following people contributed to the 2.40.0 release. Thank you to all contributors!

Ahmed Abualsaud
Ahmet Altay
Aizhamal Nurmamat kyzy
Alejandro Rodriguez-Morantes
Alexander Zhuravlev
Alexey Romanenko
Anand Inguva
andoni-guzman
Andy Ye
Balázs Németh
Benjamin Gonzalez
Brian Hulette
bulat safiullin
bullet03
Chamikara Jayalath
Damon Douglas
Daniel Oliveira
Danny McCormick
Darkhan Nausharipov
David Huntsperger
Diego Gomez
dpcollins-google
Ekaterina Tatanova
Elias Segundo
Etienne Chauchot
Evan Galpin
fbeevikm
Fernando Morales
Heejong Lee
Igor Krasavin
Ilion Beyst
Israel Herraiz
Jack McCluskey
Jan Kuehle
Jan Lukavský
johnjcasey
Jonathan Lui
jrmccluskey
Julien Tournay
Kenneth Knowles
Kerry Donny-Clark
Kevin Puthusseri
Kiley Sok
Kyle Weaver
kynx
Lucas Nogueira
Luke Cwik
LuNing Wang
Marco Robles
masahitojp
Minbo Bae
Moritz Mack
Naireen Hussain
Nancy Xu
Niel Markwick
Ning Kang
nishant jain
nishantjain91
Oskar Firlej
Pablo Estrada
pablo rodriguez defino
Rebecca Szper
Red Daly
Reuven Lax
Ritesh Ghorse
Robert Bradshaw
Robert Burke
Ryan Thompson
Sam Whittle
Thiago Nunes
Tom Stepp
vachan-shetty
Valentyn Tymofieiev
vikash2310
Vitaly Terentyev
Vladislav Chunikhin
Yichi Zhang
Yi Hu
Yiru Tang
yixiaoshen
zwestrick

