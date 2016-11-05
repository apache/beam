---
layout: default
---
<div class="alert alert-info alert-dismissible" role="alert">
<span class="glyphicon glyphicon-flag" aria-hidden="true"></span>
<button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button>
The Apache Beam project is in the process of bootstrapping. This includes the website -- so please file issues you find in <a href="{{ site.baseurl }}/use/issue-tracking/">Jira</a>. Thanks!
</div>

# Apache Beam (incubating)

Apache Beam is an open source, unified programming model that you can use to create a data processing **pipeline**. You start by building a program that defines the pipeline using one of the open source Beam SDKs. The pipeline is then executed by one of Beam's supported **distributed processing back-ends**, which include [Apache Flink](http://flink.apache.org), [Apache Spark](http://spark.apache.org), and [Google Cloud Dataflow](https://cloud.google.com/dataflow).

Beam is particularly useful for [Embarrassingly Parallel](http://en.wikipedia.org/wiki/Embarassingly_parallel) data processing tasks, in which the problem can be decomposed into many smaller bundles of data that can be processed independently and in parallel. You can also use Beam for Extract, Transform, and Load (ETL) tasks and pure data integration. These tasks are useful for moving data between different storage media and data sources, transforming data into a more desirable format, or loading data onto a new system.

## Apache Beam SDKs

The Beam SDKs provide a unified programming model that can represent and transform data sets of any size, whether the input is a finite data set from a batch data source, or an infinite data set from a streaming data source. The Beam SDKs use the same classes to represent both bounded and unbounded data, and the same transforms to operate on that data. You use the Beam SDK of your choice to build a program that defines your data processing pipeline.

Beam currently supports the following language-specific SDKs:

<table class="table table-condensed">
<tr>
  <th>Language</th>
  <th>SDK Status</th>
</tr>
<tr>
  <td>Java</td>
  <td>Active Development</td>
</tr>
<tr>
  <td>Python</td>
  <td>Coming Soon</td>
</tr>
<tr>
  <td>Other</td>
  <td>TBD</td>
</tr>
</table>

## Apache Beam Pipeline Runners

The Beam Pipeline Runners translate the data processing pipeline you define with your Beam program into the API compatible with the distributed processing back-end of your choice. When you run your Beam program, you'll need to specify the appropriate runner for the back-end where you want to execute your pipeline.

Beam currently supports Runners that work with the following distributed processing back-ends:

<table class="table table-condensed">
<tr>
  <th>Runner</th>
  <th>Status</th>
</tr>
<tr>
  <td>Google Cloud Dataflow</td>
  <td>In Development</td>
</tr>
<tr>
  <td>Apache Flink</td>
  <td>In Development</td>
</tr>
<tr>
  <td>Apache Spark</td>
  <td>In Development</td>
</tr>
</table>

**Note:** You can always execute your pipeline locally for testing and debugging purposes.

## Getting Started with Apache Beam

Interested in working with Apache Beam? Great! Here's how to get started:

* If you are interested in using Beam for your data processing tasks, use the [Get Started]({{ site.baseurl }}/get-started/) section for an overview, quickstart, and examples. Then dive into the [Documentation]({{ site.baseurl }}/documentation/) section to get up to speed on the details.
* If you're interested in contributing to the Beam SDKs or Runners, start with the [Contribute]({{ site.baseurl }}/contribute/) section.

<hr>
<div class="row">
  <div class="col-md-6">
    <h3>Blog</h3>
    <div class="list-group">
    {% for post in site.posts %}
    <a class="list-group-item" href="{{ post.url | prepend: site.baseurl }}">{{ post.date | date: "%b %-d, %Y" }} - {{ post.title }}</a>
    {% endfor %}
    </div>
  </div>
  <div class="col-md-6">
    <h3>Twitter</h3>
    <a class="twitter-timeline" href="https://twitter.com/ApacheBeam" data-widget-id="697809684422533120">Tweets by @ApacheBeam</a>
    <script>!function(d,s,id){var js,fjs=d.getElementsByTagName(s)[0],p=/^http:/.test(d.location)?'http':'https';if(!d.getElementById(id)){js=d.createElement(s);js.id=id;js.src=p+"://platform.twitter.com/widgets.js";fjs.parentNode.insertBefore(js,fjs);}}(document,"script","twitter-wjs");</script>
  </div>
</div>

<hr>

<p style="text-align:center"><img align="center" src="https://incubator.apache.org/images/apache-incubator-logo.png" alt="Apache Incubator Logo"></p>

Apache Beam is an effort undergoing incubation at [The Apache Software Foundation (ASF)](http://www.apache.org) sponsored by the Apache Incubator PMC. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.


Apache Beam (incubating) is available under [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
