---
title:  "Apache Beam 2.8.0"
date:   2018-10-29 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2018/10/29/beam-2.8.0.html
authors:
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

We are happy to present the new 2.8.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#280-2018-10-26) for this release.<!--more-->
For more information on changes in 2.8.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12343985).

## New Features / Improvements

## Known Issues

* [BEAM-4783](https://issues.apache.org/jira/browse/BEAM-4783) Performance degradations in certain situations when Spark runner is used.

### Dependency Upgrades

* Elastic Search dependency upgraded to 6.3.2
* google-cloud-pubsub dependency upgraded to 0.35.4
* google-api-client dependency upgraded to 1.24.1
* Updated Flink Runner to 1.5.3
* Updated Spark runner to Spark version 2.3.2

### SDKs

* Python SDK added support for user state and timers.
* Go SDK added support for side inputs.

### Portability

* [Python on Flink MVP](https://beam.apache.org/roadmap/portability/#python-on-flink) completed.

### I/Os

* Fixes to RedisIO non-prefix read operations.

## Miscellaneous Fixes

* Several bug fixes and performance improvements.

## List of Contributors

According to git shortlog, the following people contributed
to the 2.8.0 release. Thank you to all contributors!

Adam Horky, Ahmet Altay, Alan Myrvold, Aleksandr Kokhaniukov,
Alex Amato, Alexey Romanenko, Aljoscha Krettek, Andrew Fulton,
Andrew Pilloud, Ankur Goenka, Anton Kedin, Babu, Batkhuyag Batsaikhan, Ben Song,
Bingfeng Shu, Boyuan Zhang, Chamikara Jayalath, Charles Chen,
Christian Schneider, Cody Schroeder, Colm O hEigeartaigh, Daniel Mills,
Daniel Oliveira, Dat Tran, David Moravek, Dusan Rychnovsky, Etienne Chauchot,
Eugene Kirpichov, Gleb Kanterov, Heejong Lee, Henning Rohde, Ismaël Mejía,
Jan Lukavský, Jaromir Vanek, Jean-Baptiste Onofré, Jeff Klukas, Joar Wandborg,
Jozef Vilcek, Julien Phalip, Julien Tournay, Juta, Jára Vaněk,
Katarzyna Kucharczyk, Kengo Seki, Kenneth Knowles, Kevin Si, Kirill Kozlov,
Kyle Winkelman, Logan HAUSPIE, Lukasz Cwik, Manu Zhang, Mark Liu,
Matthias Baetens, Matthias Feys, Maximilian Michels, Melissa Pashniak,
Micah Wylde, Michael Luckey, Mike Pedersen, Mikhail Gryzykhin, Novotnik,
Petr, Ondrej Kvasnicka, Pablo Estrada, PaulVelthuis93, Pavel Slechta,
Rafael Fernández, Raghu Angadi, Renat, Reuven Lax, Robbe Sneyders,
Robert Bradshaw, Robert Burke, Rodrigo Benenson, Rong Ou, Ruoyun Huang,
Ryan Williams, Sam Rohde, Scott Wegner, Shinsuke Sugaya, Shnitz, Simon P,
Sindy Li, Stephen Lumenta, Stijn Decubber, Thomas Weise, Tomas Novak,
Tomas Roos, Udi Meiri, Vaclav Plajt, Valentyn Tymofieiev, Vitalii Tverdokhlib,
Xinyu Liu, XuMingmin, Yifan Zou, Yuan, Yueyang Qiu, aalbatross, amaliujia,
cclauss, connelloG, daidokoro, deepyaman, djhworld, flyisland, huygaa11,
jasonkuster, jglezt, kkpoon, mareksimunek, nielm, svXaverius, timrobertson100,
vaclav.plajt@gmail.com, vitaliytv, vvarma, xiliu, xinyuiscool, xitep,
Łukasz Gajowy.
