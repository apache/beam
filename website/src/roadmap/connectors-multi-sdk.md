---
layout: section
title: "Multi-SDK Connector Efforts"
permalink: /roadmap/connectors-multi-sdk/
section_menu: section-menu/roadmap.html
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

Connector-related efforts that will benefit multiple SDKs.

# Splittable DoFn
Splittable DoFn is the next generation sources framework for Beam that will
replace current frameworks for developing bounded and unbounded sources.
Splittable DoFn is being developed along side current Beam portability
efforts. See [Beam portability framework roadmap](https://beam.apache.org/roadmap/portability/) for more details.

# Cross-language transforms

As an added benefit of Beam portability efforts, in the future, we’ll be
able to utilize Beam transforms across languages. This has many benefits.
For example.

* Beam pipelines written using Python and Go SDKs will be able to utilize
the vast selection of connectors that are currently available for Java SDK.
* Java SDK will be able to utilize connectors for systems that only offer a
Python API.
* Go SDK, will be able to utilize connectors currently available for Java and
Python SDKs.
* Connector authors will be able to implement new Beam connectors using a 
language of choice and utilize these connectors from other languages reducing
the maintenance and support efforts.

See [Beam portability framework roadmap](https://beam.apache.org/roadmap/portability/) for more details.
