---
layout: default
---

# Apache Beam

#### Apache Beam provides an advanced unified programming model, allowing you to implement batch and streaming data processing jobs that can run on any execution engine.

Apache Beam is:
* **UNIFIED** - Use a single programming model for both batch and streaming use cases.
* **PORTABLE** - Execute pipelines on multiple execution environments, including Apache Apex, Apache Flink, Apache Spark, and Google Cloud Dataflow.
* **EXTENSIBLE** - Write and share new SDKs, IO connectors, and transformation libraries. 

## Get Started

To use Beam for your data processing tasks, start by reading the [Beam Overview]({{ site.baseurl }}/get-started/beam-overview) and performing the steps in the Quickstart for [Java]({{ site.baseurl }}/get-started/quickstart-java) or [Python]({{ site.baseurl }}/get-started/quickstart-py). Then dive into the [Documentation]({{ site.baseurl }}/documentation/) section for in-depth concepts and reference materials for the Beam model, SDKs, and runners.                                    

## Contribute 

Beam is an [Apache Software Foundation](http://www.apache.org) project, available under the Apache v2 license. Beam is an open source community and contributions are greatly appreciated! If you'd like to contribute, please see the [Contribute]({{ site.baseurl }}/contribute/) section.

<hr>
<div class="row">
  <div class="col-md-6">
    <h2>Blog</h2>
    <div class="list-group">
    {% for post in site.posts limit:7 %}
    <a class="list-group-item" href="{{ post.url | prepend: site.baseurl }}">{{ post.date | date: "%b %-d, %Y" }} - {{ post.title }}</a>
    {% endfor %}
    </div>
  </div>
  <div class="col-md-6">
    <h2>Twitter</h2>
    <a class="twitter-timeline" href="https://twitter.com/ApacheBeam" data-widget-id="697809684422533120">Tweets by @ApacheBeam</a>
    <script>!function(d,s,id){var js,fjs=d.getElementsByTagName(s)[0],p=/^http:/.test(d.location)?'http':'https';if(!d.getElementById(id)){js=d.createElement(s);js.id=id;js.src=p+"://platform.twitter.com/widgets.js";fjs.parentNode.insertBefore(js,fjs);}}(document,"script","twitter-wjs");</script>
  </div>
</div>
