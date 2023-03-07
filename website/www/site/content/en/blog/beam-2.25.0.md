---
title:  "Apache Beam 2.25.0"
date:   2020-10-23 14:00:00 -0800
categories:
  - blog
  - release
authors:
  - robinyq
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
We are happy to present the new 2.25.0 release of Apache Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2250-2020-10-23) for this release.<!--more-->
For more information on changes in 2.25.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12347147).

## Highlights

* Splittable DoFn is now the default for executing the Read transform for Java based runners (Direct, Flink, Jet, Samza, Twister2). The expected output of the Read transform is unchanged. Users can opt-out using `--experiments=use_deprecated_read`. The Apache Beam community is looking for feedback for this change as the community is planning to make this change permanent with no opt-out. If you run into an issue requiring the opt-out, please send an e-mail to [user@beam.apache.org](mailto:user@beam.apache.org) specifically referencing BEAM-10670 in the subject line and why you needed to opt-out. (Java) ([BEAM-10670](https://issues.apache.org/jira/browse/BEAM-10670))

## I/Os

* Added cross-language support to Java's KinesisIO, now available in the Python module `apache_beam.io.kinesis` ([BEAM-10138](https://issues.apache.org/jira/browse/BEAM-10138), [BEAM-10137](https://issues.apache.org/jira/browse/BEAM-10137)).
* Update Snowflake JDBC dependency for SnowflakeIO ([BEAM-10864](https://issues.apache.org/jira/browse/BEAM-10864))
* Added cross-language support to Java's SnowflakeIO.Write, now available in the Python module `apache_beam.io.snowflake` ([BEAM-9898](https://issues.apache.org/jira/browse/BEAM-9898)).
* Added delete function to Java's `ElasticsearchIO#Write`. Now, Java's ElasticsearchIO can be used to selectively delete documents using `withIsDeleteFn` function ([BEAM-5757](https://issues.apache.org/jira/browse/BEAM-5757)).
* Java SDK: Added new IO connector for InfluxDB - InfluxDbIO ([BEAM-2546](https://issues.apache.org/jira/browse/BEAM-2546)).

## New Features / Improvements

* Support for repeatable fields in JSON decoder for `ReadFromBigQuery` added. (Python) ([BEAM-10524](https://issues.apache.org/jira/browse/BEAM-10524))
* Added an opt-in, performance-driven runtime type checking system for the Python SDK ([BEAM-10549](https://issues.apache.org/jira/browse/BEAM-10549)).
    More details will be in an upcoming [blog post](/blog/python-performance-runtime-type-checking/index.html).
* Added support for Python 3 type annotations on PTransforms using typed PCollections ([BEAM-10258](https://issues.apache.org/jira/browse/BEAM-10258)).
    More details will be in an upcoming [blog post](/blog/python-improved-annotations/index.html).
* Improved the Interactive Beam API where recording streaming jobs now start a long running background recording job. Running ib.show() or ib.collect() samples from the recording ([BEAM-10603](https://issues.apache.org/jira/browse/BEAM-10603)).
* In Interactive Beam, ib.show() and ib.collect() now have "n" and "duration" as parameters. These mean read only up to "n" elements and up to "duration" seconds of data read from the recording ([BEAM-10603](https://issues.apache.org/jira/browse/BEAM-10603)).
* Initial preview of [Dataframes](https://s.apache.org/simpler-python-pipelines-2020#slide=id.g905ac9257b_1_21) support.
    See also example at apache_beam/examples/wordcount_dataframe.py
* Fixed support for type hints on `@ptransform_fn` decorators in the Python SDK.
  ([BEAM-4091](https://issues.apache.org/jira/browse/BEAM-4091))
  This has not enabled by default to preserve backwards compatibility; use the
  `--type_check_additional=ptransform_fn` flag to enable. It may be enabled by
  default in future versions of Beam.

## Breaking Changes

* Python 2 and Python 3.5 support dropped ([BEAM-10644](https://issues.apache.org/jira/browse/BEAM-10644), [BEAM-9372](https://issues.apache.org/jira/browse/BEAM-9372)).
* Pandas 1.x allowed.  Older version of Pandas may still be used, but may not be as well tested.

## Deprecations

* Python transform ReadFromSnowflake has been moved from `apache_beam.io.external.snowflake` to `apache_beam.io.snowflake`. The previous path will be removed in the future versions.

## Known Issues

* Dataflow streaming timers once against not strictly time ordered when set earlier mid-bundle, as the fix for  [BEAM-8543](https://issues.apache.org/jira/browse/BEAM-8543) introduced more severe bugs and has been rolled back.
* Default compressor change breaks dataflow python streaming job update compatibility. Please use python SDK version <= 2.23.0 or > 2.25.0 if job update is critical.([BEAM-11113](https://issues.apache.org/jira/browse/BEAM-11113))


## List of Contributors

According to git shortlog, the following people contributed to the 2.25.0 release. Thank you to all contributors!

Ahmet Altay, Alan Myrvold, Aldair Coronel Ruiz, Alexey Romanenko, Andrew Pilloud, Ankur Goenka,
Ayoub ENNASSIRI, Bipin Upadhyaya, Boyuan Zhang, Brian Hulette, Brian Michalski, Chad Dombrova,
Chamikara Jayalath, Damon Douglas, Daniel Oliveira, David Cavazos, David Janicek, Doug Roeper, Eric
Roshan-Eisner, Etta Rapp, Eugene Kirpichov, Filipe Regadas, Heejong Lee, Ihor Indyk, Irvi Firqotul
Aini, Ismaël Mejía, Jan Lukavský, Jayendra, Jiadai Xia, Jithin Sukumar, Jozsef Bartok, Kamil
Gałuszka, Kamil Wasilewski, Kasia Kucharczyk, Kenneth Jung, Kenneth Knowles, Kevin Puthusseri, Kevin
Sijo Puthusseri, KevinGG, Kyle Weaver, Leiyi Zhang, Lourens Naudé, Luke Cwik, Matthew Ouyang,
Maximilian Michels, Michal Walenia, Milan Cermak, Monica Song, Nelson Osacky, Neville Li, Ning Kang,
Pablo Estrada, Piotr Szuberski, Qihang, Rehman, Reuven Lax, Robert Bradshaw, Robert Burke, Rui Wang,
Saavan Nanavati, Sam Bourne, Sam Rohde, Sam Whittle, Sergiy Kolesnikov, Sindy Li, Siyuan Chen, Steve
Niemitz, Terry Xian, Thomas Weise, Tobiasz Kędzierski, Truc Le, Tyson Hamilton, Udi Meiri, Valentyn
Tymofieiev, Yichi Zhang, Yifan Mai, Yueyang Qiu, annaqin418, danielxjd, dennis, dp, fuyuwei,
lostluck, nehsyc, odeshpande, odidev, pulasthi, purbanow, rworley-monster, sclukas77, terryxian78,
tvalentyn, yoshiki.obata
