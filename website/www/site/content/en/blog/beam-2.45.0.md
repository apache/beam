---
title:  "Apache Beam 2.45.0"
date:   2023-02-15 09:00:00 -0700
categories:
- blog
- release
authors:
- johncasey
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

We are happy to present the new 2.45.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2430-2023-01-13) for this release.

<!--more-->

For more information on changes in 2.45.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/8?closed=1).

## I/Os

* MongoDB IO connector added (Go) ([#24575](https://github.com/apache/beam/issues/24575)).

## New Features / Improvements

* RunInference Wrapper with Sklearn Model Handler support added in Go SDK ([#24497](https://github.com/apache/beam/issues/23382)).
* Adding override of allowed TLS algorithms (Java), now maintaining the disabled/legacy algorithms
  present in 2.43.0 (up to 1.8.0_342, 11.0.16, 17.0.2 for respective Java versions). This is accompanied
  by an explicit re-enabling of TLSv1 and TLSv1.1 for Java 8 and Java 11.
* Add UDF metrics support for Samza portable mode.

## Breaking Changes

* Portable Java pipelines, Go pipelines, Python streaming pipelines, and portable Python batch
  pipelines on Dataflow are required to use Runner V2. The `disable_runner_v2`,
  `disable_runner_v2_until_2023`, `disable_prime_runner_v2` experiments will raise an error during
  pipeline construction. You can no longer specify the Dataflow worker jar override. Note that
  non-portable Java jobs and non-portable Python batch jobs are not impacted. ([#24515](https://github.com/apache/beam/issues/24515)).

## Bugfixes

* Avoids Cassandra syntax error when user-defined query has no where clause in it (Java) ([#24829](https://github.com/apache/beam/issues/24829)).
* Fixed JDBC connection failures (Java) during handshake due to deprecated TLSv1(.1) protocol for the JDK. ([#24623](https://github.com/apache/beam/issues/24623))
* Fixed Python BigQuery Batch Load write may truncate valid data when deposition sets to WRITE_TRUNCATE and incoming data is large (Python) ([#24623](https://github.com/apache/beam/issues/24535)).
* Fixed Kafka watermark issue with sparse data on many partitions ([#24205](https://github.com/apache/beam/pull/24205))

## List of Contributors

According to git shortlog, the following people contributed to the 2.45.0 release. Thank you to all contributors!

AdalbertMemSQL

Ahmed Abualsaud

Ahmet Altay

Alexey Romanenko

Anand Inguva

Andrea Nardelli

Andrei Gurau

Andrew Pilloud

Benjamin Gonzalez

BjornPrime

Brian Hulette

Bulat

Byron Ellis

Chamikara Jayalath

Charles Rothrock

Damon

Daniela Martín

Danny McCormick

Darkhan Nausharipov

Dejan Spasic

Diego Gomez

Dmitry Repin

Doug Judd

Elias Segundo Antonio

Evan Galpin

Evgeny Antyshev

Fernando Morales

Jack McCluskey

Johanna Öjeling

John Casey

Junhao Liu

Kanishk Karanawat

Kenneth Knowles

Kiley Sok

Liam Miller-Cushon

Lucas Marques

Luke Cwik

MakarkinSAkvelon

Marco Robles

Mark Zitnik

Melanie

Moritz Mack

Ning Kang

Oleh Borysevych

Pablo Estrada

Philippe Moussalli

Piyush Sagar

Rebecca Szper

Reuven Lax

Rick Viscomi

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Sam Whittle

Sergei Lilichenko

Seung Jin An

Shane Hansen

Sho Nakatani

Shunya Ueta

Siddharth Agrawal

Timur Sultanov

Veronica Wasson

Vitaly Terentyev

Xinbin Huang

Xinyu Liu

Xinyue Zhang

Yi Hu

ZhengLin Li

alexeyinkin

andoni-guzman

andthezhang

bullet03

camphillips22

gabihodoroaga

harrisonlimh

pablo rodriguez defino

ruslan-ikhsan

tvalentyn

yyy1000

zhengbuqian
