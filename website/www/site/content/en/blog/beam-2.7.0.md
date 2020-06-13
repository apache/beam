---
title:  "Apache Beam 2.7.0"
date:   2018-10-03 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2018/10/03/beam-2.7.0.html
authors:
        - ccy

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

We are happy to present the new 2.7.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#270-lts-2018-10-02) for this release.<!--more-->
For more information on changes in 2.7.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12343654).

## New Features / Improvements

### New I/Os

* KuduIO
* Amazon SNS sink
* Amazon SqsIO

### Dependency Upgrades

* Apache Calcite dependency upgraded to 1.17.0
* Apache Derby dependency upgraded to 10.14.2.0
* Apache HTTP components upgraded (see release notes).

### Portability

* Experimental support for Python on local Flink runner for simple
examples, see latest information [here](/contribute/portability/#status).

## Miscellaneous Fixes

### I/Os

* KinesisIO, fixed dependency issue 

## List of Contributors

According to git shortlog, the following 72 people contributed
to the 2.7.0 release. Thank you to all contributors!

Ahmet Altay, Alan Myrvold, Alexey Romanenko, Aljoscha Krettek,
Andrew Pilloud, Ankit Jhalaria, Ankur Goenka, Anton Kedin, Boyuan
Zhang, Carl McGraw, Carlos Alonso, cclauss, Chamikara Jayalath,
Charles Chen, Cory Brzycki, Daniel Oliveira, Dariusz Aniszewski,
devinduan, Eric Beach, Etienne Chauchot, Eugene Kirpichov, Garrett
Jones, Gene Peters, Gleb Kanterov, Henning Rohde, Henry Suryawirawan,
Holden Karau, Huygaa Batsaikhan, Ismaël Mejía, Jason Kuster, Jean-
Baptiste Onofré, Joachim van der Herten, Jozef Vilcek, jxlewis, Kai
Jiang, Katarzyna Kucharczyk, Kenn Knowles, Krzysztof Trubalski, Kyle
Winkelman, Leen Toelen, Luis Enrique Ortíz Ramirez, Lukasz Cwik,
Łukasz Gajowy, Luke Cwik, Mark Liu, Matthias Feys, Maximilian Michels,
Melissa Pashniak, Mikhail Gryzykhin, Mikhail Sokolov, mingmxu, Norbert
Chen, Pablo Estrada, Prateek Chanda, Raghu Angadi, Ravi Pathak, Reuven
Lax, Robert Bradshaw, Robert Burke, Rui Wang, Ryan Williams, Sindy Li,
Thomas Weise, Tim Robertson, Tormod Haavi, Udi Meiri, Vaclav Plajt,
Valentyn Tymofieiev, xiliu, XuMingmin, Yifan Zou, Yueyang Qiu.
