---
title: "Akvelon Empowers Secure and Interoperable Data Pipelines with Apache Beam"
name: "Akvelon"
icon: "/images/logos/powered-by/akvelon.png"
category: "study"
cardTitle: "Akvelon Empowers Secure and Interoperable Data Pipelines with Apache Beam"
cardDescription: "To support data privacy and pipeline reusability at scale, Akvelon developed Beam-based solutions for Protegrity and Equifax, enabling tokenization via Dataflow Flex Templates. Additionally, Akvelon built a CDAP Connector that bridges Data Fusion plugins with Apache Beam, unlocking plugin reuse and cross-runtime interoperability."
authorName: "Vitaly Terentyev"
coauthorName: "Ashley Pikle"
authorPosition: " Software Engineer @Akvelon"
coauthorPosition: "Director of AI Business Development @Akvelon"
authorImg: /images/case-study/akvelon/terentyev.png
coauthorImg: /images/case-study/akvelon/pikle.png
publishDate: 2025-13-05T00:12:00+00:00
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
        <img src="/images/logos/powered-by/akvelon.png"/>
    </div>
    <blockquote class="case-study-quote-block">
      <p class="case-study-quote-text">
        “To support data privacy and pipeline reusability at scale, Akvelon developed Beam-based solutions for Protegrity and Equifax, enabling tokenization via Dataflow Flex Templates. Additionally, Akvelon built a CDAP Connector that bridges Data Fusion plugins with Apache Beam, unlocking plugin reuse and cross-runtime interoperability.”
      </p>
      <div class="case-study-quote-author">
        <div class="case-study-quote-author-img">
            <img src="/images/case-study/akvelon/pikle.png">
        </div>
        <div class="case-study-quote-author-info">
            <div class="case-study-quote-author-name">
              Ashley Pikle
            </div>
            <div class="case-study-quote-author-position">
              Director of AI Business Development @Akvelon
            </div>
        </div>
      </div>
    </blockquote>
</div>
<div class="case-study-post">

# Akvelon Empowers Secure and Interoperable Data Pipelines with Apache Beam

## Background

To meet growing enterprise needs for secure, scalable, and interoperable data processing
pipelines, **Akvelon** developed multiple Apache Beam-powered solutions tailored for real-world
production environments:
- Data tokenization and detokenization capabilities for **Protegrity** and **Equifax**
- A connector layer to integrate **CDAP (Cloud Data Fusion)** plugins into Apache Beam
pipelines
By leveraging [Apache Beam](https://beam.apache.org/) and [Google Cloud Dataflow](https://cloud.google.com/products/dataflow?hl=en), Akvelon enabled its clients to
achieve scalable data protection, regulatory compliance, and platform interoperability through
reusable, open-source pipeline components.

## Use Case 1: Data Tokenization for Protegrity and Equifax

### The Challenge

**Protegrity**, a leader in enterprise data security, sought to enhance its data protection platform with scalable tokenization support for batch and streaming data. Their goal: allow customers like **Equifax** - a global data analytics and technology company - to tokenize sensitive data using Google Cloud Dataflow. The solution needed to be fast, secure, reusable, and compliant with privacy regulations (e.g., HIPAA, GDPR).

### The Solution

Akvelon designed and implemented a Dataflow Flex Template using Apache Beam that allows
users to tokenize and detokenize sensitive data within both batch and streaming pipelines.

### Key features:
- Seamless integration with Protegrity UDFs
- Support for multiple data formats (CSV, JSON, Parquet)
- Stateless and stateful processing using DoFn with timers for consistency and
performance
- Full compatibility with Google Cloud Dataflow optimizations

This design provided both Protegrity and Equifax with a reusable, open-source architecture for
scalable data privacy and processing.

### The Results
- Enabled data tokenization at scale for regulated industries
- Accelerated adoption of Dataflow templates across Protegrity’s customer base
- Delivered an open-source Flex Template that benefits the entire Apache Beam
community

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    In collaboration with Akvelon, Protegrity utilized a Dataflow Flex template that helps us enable customers to tokenize and detokenize streaming and batch data from a fully managed Google Cloud Dataflow service. We appreciate Akvelon’s support as a trusted partner with Google Cloud expertise.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/learner_graph.png">
    </div>
    <div class="case-study-quote-author-info">
        <div class="case-study-quote-author-name">
          Jay Chitnis
        </div>
        <div class="case-study-quote-author-position">
          VP of Partners and Business Development @Protegrity 
        </div>
    </div>
  </div>
</blockquote>

<div class="post-scheme">
    <a href="/images/case-study/akvelon/scheme-1.png" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/akvelon/scheme-1.png" alt="Protegrity & Equifax Tokenization Pipeline">
    </a>
</div>

## Use Case 2: CDAP Connector for Apache Beam

### The Challenge

**Google Cloud Data Fusion (CDAP)** had extensive plugin support for Spark but lacked native
compatibility with Apache Beam. This limitation prevented organizations from reusing CDAP's
rich ecosystem of data connectors (e.g., Salesforce, HubSpot, ServiceNow) within Beam-based
pipelines, constraining cross-platform integration.

### The Solution

Akvelon engineered a **shim layer** (CDAP Connector) that bridges CDAP plugins with Apache
Beam. This innovation enables CDAP source and sink plugins to operate seamlessly within
Beam pipelines.

### Highlights:

- Supports StructuredRecord format conversion to Beam schema (BeamRow)
- Enables mixed Spark and Beam environments using the same plugin set
- Facilitates integration testing across third-party data sources (e.g., Salesforce, Zendesk)
- Complies with Beam’s development and style guide for open-source contributions

The project included prototyping, test infrastructure, and Salesforce plugin pipelines to ensure robustness.

### The Results

- Made **CDAP plugins reusable in Beam pipelines**
- Unlocked **interoperability** between Spark and Beam runtimes
- Enabled **rapid prototyping** and plug-and-play connector reuse for Google Cloud
customers

<div class="post-scheme">
    <a href="/images/case-study/akvelon/scheme-2.png" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/akvelon/scheme-2.png" alt="CDAP Connector Integration with Apache Beam">
    </a>
</div>

## Technology Stack

- Apache Beam
- Google Cloud Dataflow
- Protegrity Data Protection Platform
- CDAP (Cloud Data Fusion)
- BigQuery
- Equifax APIs
- Salesforce, Zendesk, HubSpot, ServiceNow plugins

## Final words

Akvelon’s contributions to Apache Beam-based solutions - from advanced tokenization for
Protegrity and Equifax to enabling plugin interoperability through the CDAP Connector -
demonstrate the power of open-source, cloud-native data engineering. By delivering reusable,
scalable, and secure components, Akvelon continues to help enterprises modernize and unify
their data workflows.

## Watch the Solution in Action

[Architecture Walkthrough Video ](https://www.youtube.com/watch?v=IQIzdfNIAHk)

## About Akvelon, Inc.

Akvelon accelerates enterprise digital transformation with Google Cloud through its deep
expertise in data engineering, AI/ML, cloud infrastructure, and application development. Akvelon is a certified Google Cloud Partner.
- [Akvelon on Gogole Cloud](https://cloud.google.com/find-a-partner/partner/akvelon)
- [Akvelon Data and Analytics Accelerators](https://github.com/akvelon/DnA_accelerators)

{{< case_study_feedback "Akvelon" >}}

</div>
<div class="clear-nav"></div>
