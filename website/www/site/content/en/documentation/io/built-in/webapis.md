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

This documentation is a Work-In-Progress. See tracking issue: [#30379](https://github.com/apache/beam/issues/30379).

# Web APIs I/O connector

{{< language-switcher java >}}

The Beam SDKs include a built-in transform, called RequestResponseIO that can read from and write to Web APIs such as
REST or gRPC. Examples and discussion below focuses on the Java SDK.
Examples using Python are not yet available.
See tracking issue: [#30422](https://github.com/apache/beam/issues/30422).

## Before you start

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

{{< paragraph class="language-java" wrap="span" >}}
* [RequestResponseIO source code](https://github.com/apache/beam/tree/master/sdks/java/io/rrio)
* [RequestResponseIO Javadoc](https://beam.apache.org/releases/javadoc/current/org/apache/beam/io/requestresponse/RequestResponseIO.html)
  {{< /paragraph >}}
