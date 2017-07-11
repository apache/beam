---
layout: default
title: "Beam Releases"
permalink: get-started/downloads/
redirect_from:
  - /get-started/releases/
  - /use/releases/
  - /releases/
---

# Apache Beam&#8482; Downloads

The easiest way to use Apache Beam is via one of the released versions in a central repository.
Java SDK is available on [Maven Central Repository](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.beam%22),
and Python SDK is available on [PyPI](https://pypi.python.org/pypi/apache-beam).

For example, if you are developing using Maven and want to use the SDK for
Java with the `DirectRunner`, add the following dependencies to your
`pom.xml` file:

    <dependency>
      <groupId>org.apache.beam</groupId>
      <artifactId>beam-sdks-java-core</artifactId>
      <version>{{ site.release_latest }}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.beam</groupId>
      <artifactId>beam-runners-direct-java</artifactId>
      <version>{{ site.release_latest }}</version>
      <scope>runtime</scope>
    </dependency>

Similarly in Python, if you are using PyPI and want to use the SDK for Python with
`DirectRunner`, add the following requirement to your `setup.py` file:

    apache-beam=={{ site.release_latest }}

Additionally, you may want to depend on additional SDK modules, such as IO
connectors or other extensions, and additional runners to execute your pipeline
at scale.

## API Stability

Apache Beam uses [semantic versioning](http://semver.org/). Version numbers use the form `major.minor.incremental` and are incremented as follows:

* major version for incompatible API changes
* minor version for new functionality added in a backward-compatible manner
* incremental version for forward-compatible bug fixes

Please note that APIs marked [`@Experimental`]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/org/apache/beam/sdk/annotations/Experimental.html)
may change at any point and are not guaranteed to remain compatible across versions.

Additionally, any API may change before the first stable release, i.e., between versions denoted `0.x.y`.

## Releases

### 2.1.0
Official [source code download](https://www.apache.org/dyn/closer.cgi?filename=beam/2.1.0/apache-beam-2.1.0-source-release.zip&action=download).

[Release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12340528).

### 2.0.0
Official [source code download](https://www.apache.org/dyn/closer.cgi?filename=beam/2.0.0/apache-beam-2.0.0-source-release.zip&action=download).

[Release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12339746).

### 0.6.0
Official [source code download](https://www.apache.org/dyn/closer.cgi?filename=beam/0.6.0/apache-beam-0.6.0-source-release.zip&action=download).

[Release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12339256).

### 0.5.0
Official [source code download](https://www.apache.org/dyn/closer.cgi?filename=beam/0.5.0/apache-beam-0.5.0-source-release.zip&action=download).

[Release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12338859).

### 0.4.0
Official [source code download](https://www.apache.org/dyn/closer.cgi?filename=beam/0.4.0/apache-beam-0.4.0-source-release.zip&action=download).

[Release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12338590).

### 0.3.0-incubating
Official [source code download](https://www.apache.org/dyn/closer.cgi?filename=beam/0.3.0-incubating/apache-beam-0.3.0-incubating-source-release.zip&action=download).

[Release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12338051).

### 0.2.0-incubating
Official [source code download](https://www.apache.org/dyn/closer.cgi?filename=beam/0.2.0-incubating/apache-beam-0.2.0-incubating-source-release.zip&action=download).

[Release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12335766).

### 0.1.0-incubating
Official [source code download](https://www.apache.org/dyn/closer.cgi?filename=beam/0.1.0-incubating/apache-beam-0.1.0-incubating-source-release.zip&action=download).

The first incubating release of Apache Beam.
