---
layout: default
---
<p>
  <div class="alert alert-info alert-dismissible" role="alert">
  <span class="glyphicon glyphicon-flag" aria-hidden="true"></span>
  <button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button>
  The Apache Beam project is in the process of bootstrapping. This includes the creation of project resources, the refactoring of the initial code submission, and the formulation of project documentation, planning, and design documents. For more information about Beam see the <a href="/getting-started">getting started page</a>.
  </div>
</p>

# Apache Beam (incubating)
Apache Beam is an open source, unified model and set of language-specific SDKs for defining and executing data processing workflows, and also data ingestion and integration flows, supporting Enterprise Integration Patterns (EIPs) and Domain Specific Languages (DSLs). Beam pipelines simplify the mechanics of large-scale batch and streaming data processing and can run on a number of runtimes like Apache Flink, Apache Spark, and Google Cloud Dataflow (a cloud service). Beam also brings DSL in different languages, allowing users to easily implement their data integration processes.

## Using Apache Beam
You can use Beam for nearly any kind of data processing task, including both batch and streaming data processing. Beam provides a unified data model that can represent any size data set, including an unbounded or infinite data set from a continuously updating data source such as Kafka.

In particular, Beam pipelines can represent high-volume computations, where the steps in your job need to process an amount of data that exceeds the memory capacity of a cost-effective cluster. Beam is particularly useful for [Embarrassingly Parallel](http://en.wikipedia.org/wiki/Embarassingly_parallel) data processing tasks, in which the problem can be decomposed into many smaller bundles of data that can be processed independently and in parallel.

You can also use Beam for Extract, Transform, and Load (ETL) tasks and pure data integration. These tasks are useful for moving data between different storage media and data sources, transforming data into a more desirable format, or loading data onto a new system.

## Programming Model
Beam provides a simple and [elegant programming model](https://cloud.google.com/dataflow/model/programming-model) to express your data processing jobs. Each job is represented by a data processing pipeline that you create by writing a program with Beam. Each pipeline is an independent entity that reads some input data, performs some transforms on that data to gain useful or actionable intelligence about it, and produces some resulting output data. A pipeline’s transform might include filtering, grouping, comparing, or joining data.

Beam provides several useful abstractions that allow you to think about your data processing pipeline in a simple, logical way. Beam simplifies the mechanics of large-scale parallel data processing, freeing you from the need to manage orchestration details such as partitioning your data and coordinating individual workers.

## Key Concepts
* **Simple data representation.** Beam uses a specialized collection class, called PCollection, to represent your pipeline data. This class can represent data sets of virtually unlimited size, including bounded and unbounded data collections.
* **Powerful data transforms.** Beam provides several core data transforms that you can apply to your data. These transforms, called PTransforms, are generic frameworks that apply functions that you provide across an entire data set.
* **I/O APIs for a variety of data formats.** Beam provides APIs that let your pipeline read and write data to and from a variety of formats and storage technologies. Your pipeline can read text files, Avro files, and more.

See the [programming model documentation](https://cloud.google.com/dataflow/model/programming-model) to learn more about how Beam implements these concepts.

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

## Apache Project
Apache Beam is an [Apache Software Foundation project](http://www.apache.org),
available under the Apache v2 license.
