---
layout: default
title: "Direct Runner"
permalink: /documentation/runners/direct/
redirect_from: /learn/runners/direct/
---
# Using the Direct Runner

The Direct Runner executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model. Some of these checks include:

* enforcing immutability of elements
* enforcing encodability of elements
* elements are processed in an arbitrary order at all points
* serialization of user functions (`DoFn`, `CombineFn`, etc.)

Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners. In addition, debugging failed runs can be a non-trivial task when a pipeline executes on a remote cluster. Instead, it is often faster and simpler to perform local unit testing on your pipeline code. Unit testing your pipeline locally also allows you to use your preferred local debugging tools.

Here are some resources with information about how to test your pipelines.
* [Testing Unbounded Pipelines in Apache Beam]({{ site.baseurl }}/blog/2016/10/20/test-stream.html) talks about the use of Java classes [`PAssert`]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/testing/PAssert.html) and [`TestStream`]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/testing/TestStream.html) to test your pipelines.
* The [Apache Beam WordCount Example]({{ site.baseurl }}/get-started/wordcount-example/) contains an example of logging and testing a pipeline with [`PAssert`]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/testing/PAssert.html).


## Direct Runner prerequisites and setup

You must specify your dependency on the Direct Runner.

```java
<dependency>
   <groupId>org.apache.beam</groupId>
   <artifactId>beam-runners-direct-java</artifactId>
   <version>0.3.0-incubating</version>
   <scope>runtime</scope>
</dependency>
```

## Pipeline options for the Direct Runner

When executing your pipeline from the command-line, set `runner` to `direct`. The default values for the other pipeline options are generally sufficient.

See the reference documentation for the  <span class="language-java">[`DirectOptions`]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/runners/direct/DirectOptions.html)</span><span class="language-python">[`PipelineOptions`](https://github.com/apache/incubator-beam/blob/python-sdk/sdks/python/apache_beam/utils/options.py)</span> interface (and its subinterfaces) for defaults and the complete list of pipeline configuration options.

## Additional information and caveats

Local execution is limited by the memory available in your local environment. It is highly recommended that you run your pipeline with data sets small enough to fit in local memory. You can create a small in-memory data set using a <span class="language-java">[`Create`]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/transforms/Create.html)</span><span class="language-python">[`Create`](https://github.com/apache/incubator-beam/blob/python-sdk/sdks/python/apache_beam/transforms/core.py)</span> transform, or you can use a <span class="language-java">[`Read`]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/io/Read.html)</span><span class="language-python">[`Read`](https://github.com/apache/incubator-beam/blob/python-sdk/sdks/python/apache_beam/io/iobase.py)</span> transform to work with small local or remote files.

