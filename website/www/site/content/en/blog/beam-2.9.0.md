---
title:  "Apache Beam 2.9.0"
date:   2018-12-13 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2018/12/13/beam-2.9.0.html
authors:
        - chamikara

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

We are happy to present the new 2.9.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#290-2018-12-13) for this release.<!--more-->
For more information on changes in 2.9.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12344258).

## New Features / Improvements

### Dependency Upgrades
* Update google-api-client libraries to 1.27.0.
* Update byte-buddy to 1.9.3
* Update Flink Runner to 1.5.5
* Upgrade google-apitools to 0.5.24

### Portability

* Added support for user state and timers to Flink runner.

### I/Os

* I/O connector for RabbitMQ.
* Update SpannerIO to support unbounded writes.
* Add PFADD method to RedisIO.

### Miscellaneous Fixes
* Dataflow runner was updated to **not** use [Conscrypt](https://github.com/google/conscrypt) as the default security provider.
* Support set/delete of timers by ID in Flink runner.
* Improvements to stabilize integration tests.
* Updates Spark runner to show Beam metrics in web UI
* Vendor gRPC and Protobuf separately from beam-model-* Java packages
* Avoid reshuffle for zero and one element creates


## List of Contributors

According to git shortlog, the following people contributed
to the 2.9.0 release. Thank you to all contributors!

Adam Horky, Ahmet Altay, Alan Myrvold, Alex Amato, Alexey Romanenko, Andrea Foegler, Andrew Fulton, Andrew Pilloud, Ankur Goenka, Anton Kedin, Babu, Ben Song, Bingfeng Shu, Boyuan Zhang, Brian Martin, Brian Quinlan, Chamikara Jayalath, Charles Chen, Christian Schneider, Colm O hEigeartaigh, Cory Brzycki, CraigChambersG, Daniel Oliveira, David Moravek, Dusan Rychnovsky, Etienne Chauchot, Eugene Kirpichov, Fabien Rousseau, Gleb Kanterov, Heejong Lee, Henning Rohde, Ismaël Mejía, Jan Lukavský, Jaromir Vanek, Jason Kuster, Jean-Baptiste Onofré, Jeff Klukas, Jeroen Steggink, Julien Tournay, Jára Vaněk, Katarzyna Kucharczyk, Keisuke Kondo, Kenneth Knowles, Liam Miller-Cushon, Luke Cwik, Manu Zhang, Mark Liu, Maximilian Michels, Melissa Pashniak, Micah Wylde, Michael Luckey, Mike Pedersen, Mikhail Gryzykhin, Novotnik,  Petr, Ondrej Kvasnicka, Pablo Estrada, Pavel Slechta, Raghu Angadi, Reuven Lax, Robbe Sneyders, Robert Bradshaw, Robert Burke, Ruoyu Liu, Ruoyun Huang, Sam Rohde, Sam sam, Scott Wegner, Simon Plovyt, Thomas Weise, Tim Robertson, Tomas Novak, Udi Meiri, Vaclav Plajt, Valentyn Tymofieiev, Varun Dhussa, Vojtech Janota, Wout Scheepers, Xinyu Liu, XuMingmin, Yifan Zou, Yueyang Qiu, akedin, amaliujia, connelloG, flyisland, huygaa11, jasonkuster, jglezt, kkpoon, mareksimunek, matthiasa4, melissa, mingmxu, nielm, reuvenlax, robbe, ruoyu90, splovyt, svXaverius, vaclav.plajt@gmail.com, xinyuiscool, xitep, Łukasz Gajowy
