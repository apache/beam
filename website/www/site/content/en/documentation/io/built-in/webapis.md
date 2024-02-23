---
title: "Web Apis I/O connector"
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

[Built-in I/O Transforms](/documentation/io/built-in/)

# Web APIs I/O connector

{{< language-switcher java py >}}

The Beam SDKs include a built-in transform, called RequestResponseIO that can read from and write to Web APIs such as
REST or gRPC.

## Before you start

<!-- Java specific -->

{{< paragraph class="language-java" >}}
To use RequestResponseIO, add the Maven artifact dependency to your `pom.xml` file. See
[Maven Central](https://central.sonatype.com/artifact/org.apache.beam/beam-sdks-java-io-rrio) for available versions.
{{< /paragraph >}}

{{< highlight java >}}
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-sdks-java-io-rrio</artifactId>
    <version>{{< param release_latest >}}</version>
</dependency>
{{< /highlight >}}

## Additional resources

{{< paragraph class="language-java" >}}
Additional resources:
{{< /paragraph >}}

{{< paragraph class="language-java" >}}
* [RequestResponseIO source code](https://github.com/apache/beam/tree/master/sdks/java/io/rrio)
* [RequestResponseIO Javadoc](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)
{{< /paragraph >}}

## RequestResponseIO basics

### HTTP Web APIs

#### Reading from a GET endpoint

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/webapis/ReadFromGetEndpoint.java" webapis_read_get >}}
{{< /highlight >}}