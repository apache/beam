---
title: "Apache Beam Amplified Ricardo’s Real-time and ML Data Processing for eCommerce Platform"
name: "Ricardo"
icon: /images/logos/powered-by/ricardo.png
category: study
cardTitle: "Four Apache Technologies Combined for Fun and Profit"
cardDescription: "Ricardo, the largest online marketplace in Switzerland, uses Apache Beam to stream-process platform data and enables the Data Intelligence team to provide scalable data integration, analytics, and smart services."
authorName: "Tobias Kaymak"
authorPosition: Senior Data Engineer @ Ricardo
authorImg: /images/tobias_kaymak_photo.png
publishDate: 2021-12-01T01:36:00+00:00
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
        <img src="/images/logos/powered-by/ricardo.png"/>
    </div>
    <blockquote class="case-study-quote-block">
      <p class="case-study-quote-text">
        “Without Beam, without all this data and real time information, we could not deliver the services we are providing and handle the volumes of data we are processing.”
      </p>
      <div class="case-study-quote-author">
        <div class="case-study-quote-author-img">
            <img src="/images/tobias_kaymak_photo.png">
        </div>
        <div class="case-study-quote-author-info">
            <div class="case-study-quote-author-name">
              Tobias Kaymak
            </div>
            <div class="case-study-quote-author-position">
              Senior Data Engineer @ Ricardo
            </div>
        </div>
      </div>
    </blockquote>
</div>
<div class="case-study-post">

#  Apache Beam Amplified Ricardo’s Real-time and ML Data Processing for eCommerce Platform.

## Background

[Ricardo](https://www.ricardo.ch/) is a leading second hand marketplace in Switzerland. The site supports over 4 million
registered buyers and sellers, processing more than 6.5 million article transactions via the platform annually. Ricardo
needs to process high volumes of streaming events and manage over 5 TB of articles, assets, and analytical data.

With the scale that came from 20 years in the market, Ricardo made the decision to migrate from their on-premises data
center to cloud to easily grow and evolve further and reduce operational costs through managed cloud services. Data
intelligence and engineering teams took the lead on this transformation and development of new AI/ML-enabled customer
experiences. Apache Beam has been a technology amplifier that expedited Ricardo’s transformation.

## Challenge

Migrating from an on-premises data center to the cloud presented Ricardo with an opportunity to modernize their
marketplace from heavy legacy reliance on transactional SQL, switch to BigQuery for analytics, and take advantage of the
event-based streaming architecture.

Ricardo’s data intelligence team identified two key success factors: a carefully designed data model and a framework
that provides unified stream and batch data pipelines execution, both on-premises and in the cloud.

Ricardo needed a data processing framework that can scale easily, enrich event streams with historic data from multiple
sources, provide granular control on data freshness, and provide an abstract pipeline operational infrastructure, thus
helping their team focus on creating new value for customers and business

## Journey to Beam

Ricardo’s data intelligence team began modernizing their stack in 2018. They selected frameworks that provide reliable
and scalable data processing both on-premises and in the cloud. Apache Beam enables users to create pipelines in their
favorite programming language offering SDKs in Java, Python, Go, SQL, Scala (SCIO).
A [Beam Runner](/documentation/#available-runners) runs a Beam pipeline on a specific (often
distributed) data processing system. Ricardo selected the Apache Beam Flink runner for executing pipelines on-premises
and the Dataflow runner as a managed cloud service for the same pipelines developed using Apache Beam Java SDK. Apache
Flink is well known for its reliability and cost-efficiency and an on-premises cluster was spun up at Ricardo’s
datacenter as the initial environment.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    We wanted to implement a solution that would multiply our possibilities, and that’s exactly where Beam comes in. One of the major drivers in this decision was the ability to evolve without adding too much operational load.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/tobias_kaymak_photo.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Tobias Kaymak
        </div>
        <div class="case-study-quote-author-position">
          Senior Data Engineer @ Ricardo
        </div>
    </div>
  </div>
</blockquote>

Beam pipelines for core business workloads to ingest events data from Apache Kafka into BigQuery were running stable in
just one month. As Ricardo’s cloud migration progressed, the data intelligence
team [migrated Flink cluster from Kubernetes](https://www.youtube.com/watch?v=EcvnFH5LDE4) in their on-premises
datacenter to GKE.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    I knew Beam, I knew it works. When you need to move from Kafka to BigQuery and you know that Beam is exactly the right tool, you just need to choose the right executor for it.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/tobias_kaymak_photo.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Tobias Kaymak
        </div>
        <div class="case-study-quote-author-position">
          Senior Data Engineer @ Ricardo
        </div>
    </div>
  </div>
</blockquote>

The flexibility to refresh data every hour, minute, or stream data real-time, depending on the specific use case and
need, helped the team improve data freshness which was a significant advancement for Ricardo’s eCommerce platform
analytics and reporting.

Ricardo’s team found benefits in Apache Beam Flink runner on self-managed Flink cluster in GKE for streaming pipelines.
Full control over Flink provisioning enabled to set up required connectivity from Flink cluster to an external peered
Kafka managed service. The data intelligence team optimized operating costs through cluster resource utilization
significantly. For batch pipelines, the team chose Dataflow managed service for its on-demand autoscaling and cost
reduction features like FlexRS, especially efficient for training ML models over TBs of historic data. This hybrid
approach has been serving Ricardo’s needs well and proved to be a reliable production solution.

## Evolution of Use Cases

Thinking of a stream as data in motion, and a table as data at rest provided a fortuitous chance to take a look at some
data model decisions that were made as far back as 20 years before. Articles that are on the marketplace have assets
that describe them, and for performance and cost optimizations purposes, data entities that belong together were split
into separate database instances. Apache Beam enabled Ricardo’s data intelligence team
to [join assets and articles streams](https://youtu.be/PiwLC-YK_Zw) and optimize BigQuery scans to reduce costs. When
designing the pipeline, the team created streams for assets and articles. Since the assets stream is the primary one,
they shifted the stream 5 minutes back and created a lookup schema with it in BigTable. This elegant solution ensures
that the assets stream is always processed first while BigTable allows for matching the latest asset to an article and
Apache Beam joins them both together.

<div class="post-scheme">
    <img src="/images/post_scheme.png">
</div>

The successful case of joining different data streams facilitated further Apache Beam adoption by Ricardo in areas like
data science and ML.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    Once you start laying out the simple use cases, you will always figure out the edge case scenarios. This pipeline has been running for a year now, and Beam handles it all, from super simple use cases to something crazy.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/tobias_kaymak_photo.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Tobias Kaymak
        </div>
        <div class="case-study-quote-author-position">
          Senior Data Engineer @ Ricardo
        </div>
    </div>
  </div>
</blockquote>

As an eCommerce retailer, Ricardo faces the increasing scale and sophistication of fraud transactions and takes a
strategic approach by employing Beam pipelines for fraud detection and prevention. Beam pipelines act on an external
intelligent API to identify the signs of fraudulent behaviour, like device characteristics or user activity. Apache Beam
[stateful processing](/documentation/programming-guide/#state-and-timers) feature enables Ricardo
to apply an associating operation to the streams of data (trigger banishing a user for example). Thus, Apache Beam saves
Ricardo’s customer care team's time and effort on investigating duplicate cases. It also runs batch pipelines
to [find linked accounts](https://www.youtube.com/watch?v=LXnh9jNNfYY), associate products to categories by
encapsulating a ML model, or calculates the likelihood something is going to sell, at a scale or precision that was
previously not possible.

Originally implemented by Ricardo’s data intelligence team, Apache Beam has proven to be a powerful framework that
supports advanced scenarios and acts as a glue between Kafka, BigQuery, and platform and external APIs, which encouraged
other teams at Ricardo to adopt it.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    [Apache Beam] is a framework that is so good that other teams are picking up the idea and starting to work with it after we tested it.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/tobias_kaymak_photo.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Tobias Kaymak
        </div>
        <div class="case-study-quote-author-position">
          Senior Data Engineer @ Ricardo
        </div>
    </div>
  </div>
</blockquote>

## Results

Apache Beam has provided Ricardo with a scalable and reliable data processing framework that supported Ricardo’s
fundamental business scenarios and enabled new use cases to respond to events in real-time.

Throughout Ricardo’s transformation, Apache Beam has been a unified framework that can run batch and stream pipelines,
offers on-premises and cloud managed services execution, and programming language options like Java and Python,
empowered data science and research teams to advance customer experience with new real-time scenarios fast-tracking time
to value.

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    After this first pipeline, we are working on other use cases and planning to move them to Beam. I was always trying to spread the idea that this a framework that is reliable, it actually helps you to get the stuff done in a consistent way.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/tobias_kaymak_photo.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Tobias Kaymak
        </div>
        <div class="case-study-quote-author-position">
          Senior Data Engineer @ Ricardo
        </div>
    </div>
  </div>
</blockquote>

Apache Beam has been a technology that multiplied possibilities, allowing Ricardo to maximize technology benefits at all
stages of their modernization and cloud journey.

## Learn More

<iframe class="video video--medium-size" width="560" height="315" src="https://www.youtube.com/embed/v-MclVrGJcQ" frameborder="0" allowfullscreen></iframe>
<br><br>

{{< case_study_feedback Ricardo >}}

</div>
<div class="clear-nav"></div>
