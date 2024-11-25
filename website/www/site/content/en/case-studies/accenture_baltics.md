---
title: "Accenture Baltics' Journey with Apache Beam to Streamlined Data Workflows for a Sustainable Energy Leader"
name: "Accenture Baltics"
icon: /images/logos/powered-by/accenture.png
hasNav: true
category: study
cardTitle: "Accenture Baltics' Journey with Apache Beam"
cardDescription: "Accenture Baltics uses Apache Beam on Google Cloud to build a robust data processing infrastructure for a sustainable energy leader.They use Beam to democratize data access, process data in real-time, and handle complex ETL tasks."
authorName: "Jana Polianskaja"
authorPosition: "Data Engineer @ Accenture Baltics"
authorImg: /images/case-study/accenture/Jana_Polianskaja_sm.jpg
publishDate: 2024-11-25T00:12:00+00:00
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
<!-- div with class case-study-opinion is displayed at the top left area of the case study page -->
<div class="case-study-opinion">
    <div class="case-study-opinion-img">
        <img src="/images/logos/powered-by/accenture.png"/>
    </div>
    <blockquote class="case-study-quote-block">
      <p class="case-study-quote-text">
        “Apache Beam empowers team members who don’t have data engineering backgrounds to directly access and analyze BigQuery data by using SQL. The data scientists, the finance department, and production optimization teams all benefit from improved data accessibility, which gives them immediate access to critical information for faster analysis and decision-making.”
      </p>
      <div class="case-study-quote-author">
        <div class="case-study-quote-author-img">
            <img src="/images/case-study/accenture/Jana_Polianskaja_sm.jpg">
        </div>
        <div class="case-study-quote-author-info">
            <div class="case-study-quote-author-name">
              Jana Polianskaja
            </div>
            <div class="case-study-quote-author-position">
              Data Engineer @ Accenture Baltics
            </div>
        </div>
      </div>
    </blockquote>
</div>

<!-- div with class case-study-post is the case study page main content -->
<div class="case-study-post">

# Accenture Baltics' Journey with Apache Beam to Streamlined Data Workflows for a Sustainable Energy Leader

## Background

Accenture Baltics, a branch of the global professional services company Accenture, leverages its expertise across various industries to provide consulting, technology, and outsourcing solutions to clients worldwide. A specific project at Accenture Baltics highlights the effective implementation of Apache Beam to support a client who is a global leader in sustainable energy and uses Google Cloud.

## Journey to Apache Beam

The team responsible for transforming, curating, and preparing data, including transactional, analytics, and sensor data, for data scientists and other teams has been using Dataflow with Apache Beam for about five years. Dataflow with Beam is a natural choice for both streaming and batch data processing. For our workloads, we typically use the following configurations: worker machine types are `n1-standard-2` or `n1-standard-4`, and the maximum number of workers varies up to five, using the Dataflow runner.

As an example, a streaming pipeline ingests transaction data from Pub/Sub, performs basic ETL and data cleaning, and outputs the results to BigQuery. A separate batch Dataflow pipeline evaluates a binary classification model, reading input and writing results to Google Cloud Storage. The following diagram shows a workflow that uses Pub/Sub to feed Dataflow pipelines across three Google Cloud projects. It also shows how Dataflow, Composer, Cloud Storage, BigQuery, and Grafana integrate into the architecture.

<div class="post-scheme">
    <a href="/images/case-study/accenture/dataflow_pipelines.png" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/accenture/dataflow_pipelines.png" alt="Diagram of Accenture Baltics' Dataflow pipeline architecture">
    </a>
</div>

## Use Cases

Apache Beam is an invaluable tool for our use cases, particularly in the following areas:

* **Democratizing data access:** Beam empowers team members without data engineering backgrounds to directly access and analyze BigQuery data using their SQL skills. The data scientists, the finance department, and production optimization teams all benefit from improved data accessibility, gaining immediate access to critical information for faster analysis and decision-making.
* **Real-time data processing:** Beam excels at ingesting and processing data in real time from sources like Pub/Sub.
* **ETL (extract, transform, load):** Beam effectively manages the full spectrum of data transformation and cleaning tasks, even when dealing with complex data structures.
* **Data routing and partitioning:** Beam enables sophisticated data routing and partitioning strategies. For example, it can automatically route failed transactions to a separate BigQuery table for further analysis.
* **Data deduplication and error handling:** Beam has been instrumental in tackling challenging tasks like deduplicating Pub/Sub messages and implementing robust error handling, such as for JSON parsing, that are crucial for maintaining data integrity and pipeline reliability.

We also utilize Grafana (shown in below) with custom notification emails and tickets for comprehensive monitoring of our Beam pipelines. Notifications are generated from Google’s Cloud Logging and Cloud Monitoring services to ensure we stay informed about the performance and health of our pipelines. The seamless integration of Airflow with Dataflow and Beam further enhances our workflow, allowing us to effortlessly use operators such as `DataflowCreatePythonJobOperator` and `BeamRunPythonPipelineOperator` in [Airflow 2](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataflow/index.html).

<div class="post-scheme">
    <a href="/images/case-study/accenture/dataflow_grafana.jpg" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/accenture/dataflow_grafana.jpg" alt="scheme">
    </a>
</div>

## Results

Our data processing infrastructure uses 12 distinct pipelines to manage and transform data across various projects within the organization. These pipelines are divided into two primary categories:

* **Streaming pipelines:** These pipelines are designed to handle real-time or near real-time data streams. In our current setup, these pipelines process an average of 10,000 messages per second from Pub/Sub and about 200,000 rows per hour to BigQuery, ensuring that time-sensitive data is ingested and processed with minimal latency.
* **Batch pipelines:** These pipelines are optimized for processing large volumes of data in scheduled batches. Our current batch pipelines handle approximately two gigabytes of data per month, transforming and loading this data into our data warehouse for further analysis and reporting.

Apache Beam has proven to be a highly effective solution for orchestrating and managing the complex data pipelines required by the client. By leveraging the capabilities of Dataflow, a fully managed service for executing Beam pipelines, we have successfully addressed and fulfilled the client's specific data processing needs. This powerful combination has enabled us to achieve scalability, reliability, and efficiency in handling large volumes of data, ensuring timely and accurate delivery of insights to the client.

*Check out [my Medium blog](https://medium.com/@jana_om)\! I usually post about using Beam/Dataflow as an ETL tool with Python and how it works with other data engineering tools. My focus is on building projects that are easy to understand and learn from, especially if you want to get some hands-on experience with Beam.*

<!-- case_study_feedback adds feedback buttons -->
{{< case_study_feedback "AccentureBalticsStreaming" >}}
</div>
<div class="clear-nav"></div>
