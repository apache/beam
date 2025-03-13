---
title: "Beam visual pipeline development with Hop"
name: "Neo4j"
icon: /images/logos/powered-by/hop.png
category: study
cardTitle: "Visual Apache Beam Pipeline Design and Orchestration with Apache Hop"
cardDescription: "Apache Hop is an open source data orchestration and engineering platform that extends Apache Beam with visual pipeline lifecycle management. Neo4j’s Chief Solution Architect and Apache Hop’s co-founder, Matt Casters, sees Apache Beam as a driving force behind Hop."
authorName: "Matt Casters"
authorPosition: "Chief Solutions Architect, Neo4j, Apache Hop co-founder"
authorImg: /images/matt_casters_photo.png
publishDate: 2022-02-15T12:21:00+00:00
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

<div class="case-study-opinion">
    <div class="case-study-opinion-img">
        <img class="case-study-opinion-img-center" src="/images/logos/powered-by/hop.png"/>
    </div>
    <blockquote class="case-study-quote-block">
      <p class="case-study-quote-text">
        “Apache Beam and its abstraction of the execution engines is a big thing for us. The amount of work that that saves...it would be hard to build that support for Dataflow or Spark all by yourself. It is amazing that this technology exists in the first place, really amazing! Not having to worry about all those underlying platforms - that is tremendous!”
      </p>
      <div class="case-study-quote-author">
        <div class="case-study-quote-author-img">
            <img src="/images/matt_casters_photo.png">
        </div>
        <div class="case-study-quote-author-info">
            <div class="case-study-quote-author-name">
              Matt Casters
            </div>
            <div class="case-study-quote-author-position">
              Chief Solutions Architect, Neo4j, Apache Hop co-founder
            </div>
        </div>
      </div>
    </blockquote>
</div>
<div class="case-study-post">

# Visual Apache Beam Pipeline Design and Orchestration with Apache Hop

## Background

[Apache Hop](https://hop.apache.org/) is an open source data orchestration and data engineering
platform that aims to facilitate all aspects of data processing with visual pipeline development
environment. This easy-to-use, fast, and flexible platform enables developers to create and manage
Apache Beam batch and streaming pipelines in Hop GUI. Apache Hop uses metadata and kernel to
describe how the data should be processed, and Apache Beam to “design once, run anywhere”.

[Neo4j’s](https://neo4j.com/) Chief Solutions
Architect, [Matt Casters](https://be.linkedin.com/in/mattcasters), has been an early adopter of
Apache Beam and its abstraction of execution engines. Matt has been an active member of the Apache
open-source community for years and has leveraged Apache Beam as an execution engine to build Apache
Hop.

## Apache Hop Project

Thriving popularity and the growing number of Apache Beam users across the globe inspired Matt
Casters to expand the idea of abstraction to visual pipeline lifecycle management and development.
Matt co-founded and incubated the
[Apache Hop](https://hop.apache.org/) project that became a top level project at
the [Apache Software Foundation](https://www.apache.org/)
in December 2021. The platform enables users of all skill levels to build, test, launch, and deploy
powerful data workflows without writing code. Apache Hop’s intuitive drag and drop interface
provides a visual representation of Apache Beam pipelines, simplifying pipeline design, execution,
preview, monitoring, and debugging.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    I was a big fan of Beam from the get go. Apache Beam is now a very important part of the Apache Hop project.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/matt_casters_photo.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Matt Casters
        </div>
        <div class="case-study-quote-author-position">
          Chief Solutions Architect, Neo4j,
          <br>Apache Hop co-founder
        </div>
    </div>
  </div>
</blockquote>

The Apache Hop GUI allows data professionals to work visually and focus on “what” they need to do
rather than “how”, using metadata to describe how the Apache Beam pipelines should be processed.
Apache
Hop’s [transform-agnostic](https://hop.apache.org/manual/latest/pipeline/create-pipeline.html#_concepts)
action
plugins ([“hops”](https://hop.apache.org/manual/latest/pipeline/create-pipeline.html#_concepts))
link transforms together, creating a pipeline. Various Apache Beam runners, such as
[Spark](https://hop.apache.org/manual/latest/pipeline/pipeline-run-configurations/beam-spark-pipeline-engine.html)
,
[Flink](https://hop.apache.org/manual/latest/pipeline/pipeline-run-configurations/beam-flink-pipeline-engine.html)
,
[Dataflow](https://hop.apache.org/manual/latest/pipeline/pipeline-run-configurations/beam-dataflow-pipeline-engine.html)
, and
the [Direct](https://hop.apache.org/manual/latest/pipeline/pipeline-run-configurations/beam-direct-pipeline-engine.html)
runner, read the metadata with help of Apache
Hop's [Metadata Provider](https://hop.apache.org/dev-manual/latest/sdk/hop-sdk.html#_hop_metadata_providers)
and [workflow engines(plugins)](https://hop.apache.org/dev-manual/latest/sdk/hop-sdk.html#_workflow_execution)
, and execute the pipeline.

Apache Hop’s custom plugins and metadata objects
for [some of the most popular technologies](https://hop.apache.org/manual/latest/technology/technology.html)
, such as [Neo4j](https://neo4j.com/), empower users to execute database- and technology-specific
transforms inside the Apache Beam pipelines, which allows for native optimized connectivity and
flexible Apache Beam pipeline configurations. For instance, the Apache
Hop’s [Neo4j plugin](https://hop.apache.org/manual/latest/technology/neo4j/index.html#_description)
stores logging and execution lineage of Apache Beam pipelines in the Neo4j graph database and
enables users to query this information for more details, such as quickly jump to the place where an
error occurred. The combination of Apache Hop
transforms, [Apache Beam built-in I/Os](/documentation/io/built-in/), and
Apache Beam-powered data processing opens up new horizons for more sinks and sources and custom use
cases.

Apache Hop aims to bring a no-code approach to Apache Beam data pipelines. Sometimes the choice of a
particular programming language, framework, or engine is driven by developers' preferences, which
results in businesses becoming tied to a specific technology skill set and stack. Apache Hop
eliminates this dependency by abstracting out the I/Os with a fully pluggable runtime support and
providing a graphic user interface on top of Apache Beam pipelines. All settings for pipeline
elements are performed in the Hop’s visual editor just once, and pipeline is automatically described
as metadata in JSON and CSV formats. Programming data pipelines’ source code becomes an option, not
a necessity. Apache Hop does not require knowledge of a particular programming language to create
pipelines, helping with the adoption of Apache Beam unified streaming and batch processing
technology.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    In general, a visual pipeline design interface is really valuable for a non-developer audience…
    We categorically choose the side of the organization when it comes to lowering setup costs,
    maintenance costs, increasing ROI, and safeguarding an investment over time.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/matt_casters_photo.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Matt Casters
        </div>
        <div class="case-study-quote-author-position">
          Chief Solutions Architect, Neo4j,
          <br>Apache Hop co-founder
        </div>
    </div>
  </div>
</blockquote>

## Results

Apache Beam continuously expands the number of use cases and scenarios it supports and makes it
possible to bring advanced technology solutions into a reality. Being an early adopter of Apache
Beam and its powerful abstraction, Matt Casters leveraged this knowledge and experience to create
Apache Hop. The platform creates a value-add for Apache Beam users by enabling visual pipeline
development and lifecycle management.

Matt sees Apache Beam as a foundation and a driving force behind Apache Hop. Communication between
Apache Beam and Apache Hop projects keeps fostering co-creation and enriches both products with new
features.

Apache Hop project is the example of the continuous improvement driven by the Apache open source
community and amplified by collaborative organizations.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    Knowledge sharing and collaboration is something that comes naturally in the community. If we
    see some room for improvement, we exchange ideas and this way, we keep driving Apache Beam and
    Apache Hop projects forward. Together, we can work with the most complex problems and just solve them.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/matt_casters_photo.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Matt Casters
        </div>
        <div class="case-study-quote-author-position">
          Chief Solutions Architect, Neo4j,
          <br>Apache Hop co-founder
        </div>
    </div>
  </div>
</blockquote>
{{< case_study_feedback Hop >}}

</div>
<div class="clear-nav"></div>
