---
layout: section
title: "Apache Beam Roadmap Highlights"
permalink: /roadmap/
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

# Apache Beam Roadmap

Apache Beam is not governed or steered by any one commercial entity, but by its
Project Management Committee (PMC), so we do not have a roadmap in the sense of
a plan with a specific timeline.
Instead, we share our vision for the future and major initiatives receiving
and attention and contribution that users can look forward to. 

The major components of Beam each have their own roadmap which you can find
via the menu.
Below are some highlights for the project as a whole.

## Portability Framework

Portability is the primary Beam vision: running pipelines authors with _any SDK_
on _any runner_. This is a cross-cutting effort across Java, Python, and Go, 
and every Beam runner.

See the details on the [Portability Roadmap]({{site.baseurl}}/roadmap/portability/)

## Python on Flink

A major highlight of the portability effort is the effort in running Python pipelines
the Flink runner.

You can [follow the instructions to try it out]({{site.baseurl}}/roadmap/portability/#python-on-flink)

## Go SDK

The Go SDK is the newest SDK, and is the first SDK built entirely on the
portability framework. See the [Go SDK's Roadmap]({{site.baseurl}}/roadmap/go-sdk) if this piques your
interest.

## Python 3 support

The Python SDK is near to supporting Python 3. See details on 
the [Python SDK's Roadmap]({{site.baseurl}}/roadmap/python-sdk/#python-3-support).
This is also a great opportunity for lightweight contribution!

## Java 11 support

The Java SDK is eager to add support for Java's first new LTS (Long Term Support)
version. See details on 
the [Java SDK's Roadmap]({{site.baseurl}}/roadmap/java-sdk).

## SQL

Beam's SQL module is rapidly maturing to allow users to author batch and
streaming pipelines using only SQL, but also to allow Beam Java developers
to use SQL in components of their pipeline for added efficiency. See the 
[Beam SQL Roadmap]({{site.baseurl}}/roadmap/sql/)

## Euphoria

Euphoria is Beam's newest API, offering a high-level, fluent style for
Beam Java developers. See the [Euphoria API Roadmap]({{site.baseurl}}/roadmap/euphoria).

