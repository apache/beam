---
title: "Apache Beam: IO Standards for Syntax and semantics"
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
# Beam I/O Standards: API Syntax and Semantics

## Overview

This outlines a set of guidelines for Apache Beam's IO connectors. These
guidelines define the comunity's best practices to ease the review, development,
maintenance and usage of new connectors.

These guidelines are written with the following principles in mind:

- Consistency makes an API easier to learn
- If there are multiple ways of doing something, we shall encourage one for consistency.
- With a couple minutes of studying documentation, users should be able to pick up most IO connectors
- The design of a new IO should consider the possibility of evolution
- Transforms should integrate well with other Beam utilities


## Java

### General

| G1 | If the IO lives in Apache Beam it should be placed in the package :

org.apache.beam.sdk.io.{connector}

<br>

eg: org.apache.beam.sdk.io.bigquery                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| G2 | There will be a class named {connector}IO which is the primary class used in working with the connector in a pipeline.

eg: org.apache.beam.sdk.io.bigquery.BigQueryIO                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| G3 | Given that the IO will be placed in the package org.apache.beam.sdk.io.{connector}, it is also suggested that its integration tests will live under the package org.apache.beam.sdk.io.{connector}.testing. This will cause integration tests to work with the standard user-facing interfaces of the connector.

<br>

Unit tests should reside in the same package (i.e. org.apache.beam.sdk.io.{connector}), as they may often test internals of the connector.                                                                                                                                                                                                                                                                                                                                                                                 |
| G4 | An IO transform should avoid receiving user lambdas to map elements from a user type to a connector-specific type. Instead, they should interface with a connector-specific data type (with schema information when possible).

<br>

When necessary, then an IO transform should receive a type parameter that specifies the input type (for sinks) or output type (for sources) of the transform.

<br>

An IO transform may not have a type parameter only if it is certain that its output type will not change (e.g. [FileIO.MatchAll](https://beam.apache.org/releases/javadoc/2.34.0/org/apache/beam/sdk/io/FileIO.MatchAll.html) and other [FileIO transforms](https://beam.apache.org/releases/javadoc/2.34.0/org/apache/beam/sdk/io/FileIO.html)).                                                                                       |
| G5 | As part of the API of a Beam IO connector, it is discouraged to expose third-party libraries in the public API of a Beam API or connector. Instead, a Beam-native interface that can be translated into the third-library object is preferrable. This is because:

*   Exposing third-party libraries will make dependency upgrades difficult, because of Beam’s backwards compatibility guarantees
    
*   Exposing third-party libraries may force dependencies onto Beam users
    

<br>

In cases where the external library is complex, or changes frequently, the connector may expose library-specific classes in its API.

<br>

This requirement has the shortcoming that Beam will simply mirror external library API objects, but it will allow us to upgrade external libraries if needed, and let users deal with versions as well. |
