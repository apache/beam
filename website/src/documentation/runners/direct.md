---
layout: section
title: "Direct Runner"
permalink: /documentation/runners/direct/
section_menu: section-menu/runners.html
redirect_from: /learn/runners/direct/
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
# Using the Direct Runner

<nav class="language-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="language-java" class="active">Java SDK</li>
    <li data-type="language-py">Python SDK</li>
  </ul>
</nav>

The Direct Runner executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model. Some of these checks include:

* enforcing immutability of elements
* enforcing encodability of elements
* elements are processed in an arbitrary order at all points
* serialization of user functions (`DoFn`, `CombineFn`, etc.)

Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners. In addition, debugging failed runs can be a non-trivial task when a pipeline executes on a remote cluster. Instead, it is often faster and simpler to perform local unit testing on your pipeline code. Unit testing your pipeline locally also allows you to use your preferred local debugging tools.

Here are some resources with information about how to test your pipelines.
<ul>
  <!-- Java specific links -->
  <li class="language-java"><a href="{{ site.baseurl }}/blog/2016/10/20/test-stream.html">Testing Unbounded Pipelines in Apache Beam</a> talks about the use of Java classes <a href="https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/testing/PAssert.html">PAssert</a> and <a href="https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/testing/TestStream.html">TestStream</a> to test your pipelines.</li>
  <li class="language-java">The <a href="{{ site.baseurl }}/get-started/wordcount-example/#testing-your-pipeline-with-asserts">Apache Beam WordCount Walkthrough</a> contains an example of logging and testing a pipeline with <a href="https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/testing/PAssert.html">PAssert</a>.</li>

  <!-- Python specific links -->
  <li class="language-py">The <a href="{{ site.baseurl }}/get-started/wordcount-example/#testing-your-pipeline-with-asserts">Apache Beam WordCount Walkthrough</a> contains an example of logging and testing a pipeline with <a href="https://beam.apache.org/releases/pydoc/{{ site.release_latest }}/apache_beam.testing.util.html#apache_beam.testing.util.assert_that">assert_that</a>.</li>
</ul>

## Direct Runner prerequisites and setup

### Specify your dependency

<span class="language-java">When using Java, you must specify your dependency on the Direct Runner in your `pom.xml`.</span>
```java
<dependency>
   <groupId>org.apache.beam</groupId>
   <artifactId>beam-runners-direct-java</artifactId>
   <version>{{ site.release_latest }}</version>
   <scope>runtime</scope>
</dependency>
```

<span class="language-py">This section is not applicable to the Beam SDK for Python.</span>

## Pipeline options for the Direct Runner

When executing your pipeline from the command-line, set `runner` to `direct` or `DirectRunner`. The default values for the other pipeline options are generally sufficient.

See the reference documentation for the
<span class="language-java">[`DirectOptions`](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/runners/direct/DirectOptions.html)</span>
<span class="language-py">[`DirectOptions`](https://beam.apache.org/releases/pydoc/{{ site.release_latest }}/apache_beam.options.pipeline_options.html#apache_beam.options.pipeline_options.DirectOptions)</span>
interface for defaults and additional pipeline configuration options.

## Additional information and caveats

### Memory considerations

Local execution is limited by the memory available in your local environment. It is highly recommended that you run your pipeline with data sets small enough to fit in local memory. You can create a small in-memory data set using a <span class="language-java">[`Create`](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/transforms/Create.html)</span><span class="language-py">[`Create`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py)</span> transform, or you can use a <span class="language-java">[`Read`](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/io/Read.html)</span><span class="language-py">[`Read`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/iobase.py)</span> transform to work with small local or remote files.

### Streaming execution

If your pipeline uses an unbounded data source or sink, you must set the `streaming` option to `true`.


