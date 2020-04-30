---
title: "Release blockers"
aliases: /contribute/release-blockers/
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

# Release blockers

A release blocking Jira is any open Jira that has its `Fix Version` field set
to an upcoming version of Beam.

## Release-blocking bugs

A bug should block a release if it is a significant regression or loss of
functionality. It should usually have priority Critical/P1. Lower priorities do
not have urgency, while Blocker/P0 is reserved for issues so urgent they do not
wait for a release.

## Release-blocking features

By default, features do not block releases. Beam has a steady 6 week cadence of
cutting release branches and releasing. Features "catch the train" or else wait
for the next release.

A feature can block a release if there is community consensus to delay a
release in order to include the feature.

