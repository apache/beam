---
layout: default
title: "Apache Beam Releases"
permalink: /releases/
---

# Apache Beam releases

The easiest way to use Apache Beam is via one of the released versions in the
[Maven Central Repository](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.beam%22).

For example, if you are developing using Maven and want to use the SDK for
Java with the `DirectRunner`, add the following dependencies to your
`pom.xml` file:

    <dependency>
      <groupId>org.apache.beam</groupId>
      <artifactId>beam-sdks-java-core</artifactId>
      <version>0.1.0-incubating</version>
    </dependency>
    <dependency>
      <groupId>org.apache.beam</groupId>
      <artifactId>beam-runners-direct-java</artifactId>
      <version>0.1.0-incubating</version>
      <scope>runtime</scope>
    </dependency>

Additionally, you may want to depend on additional SDK modules, such as IO
connectors or other extensions, and additional runners to execute your pipeline
at scale.

## Release Notes

### 0.1.0-incubating
[Source code download](https://dist.apache.org/repos/dist/release/incubator/beam/0.1.0-incubating/apache-beam-0.1.0-incubating-source-release.zip)

* The first incubating release of Apache Beam.
