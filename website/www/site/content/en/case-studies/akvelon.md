---
title: "Secure and Interoperable Apache Beam Pipelines by Akvelon"
name: "Akvelon"
icon: "/images/logos/powered-by/akvelon.png"
category: "study"
cardTitle: "Secure and Interoperable Apache Beam Pipelines by Akvelon"
cardDescription: "To support data privacy and pipeline reusability at scale, Akvelon developed Beam-based solutions for Protegrity and a major North American credit reporting company, enabling tokenization with Dataflow Flex Templates. Akvelon also built a CDAP Connector to integrate CDAP plugins with Apache Beam, enabling plugin reuse and multi-runtime compatibility."
authorName: "Vitaly Terentyev"
coauthorName: "Ashley Pikle"
authorPosition: "Software Engineer @Akvelon"
coauthorPosition: "Director of AI Business Development @Akvelon"
authorImg: /images/case-study/akvelon/terentyev.png
coauthorImg: /images/case-study/akvelon/pikle.png
publishDate: 2025-05-25T00:12:00+00:00
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
        “To support data privacy and pipeline reusability at scale, Akvelon developed Beam-based solutions for Protegrity and a major North American credit reporting company, enabling tokenization with Dataflow Flex Templates. Akvelon also built a CDAP Connector to integrate CDAP plugins with Apache Beam, enabling plugin reuse and multi-runtime compatibility.”
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

# Secure and Interoperable Apache Beam Pipelines by Akvelon

## Background

To meet growing enterprise needs for secure, scalable, and interoperable data processing pipelines, **Akvelon** developed multiple Apache Beam-powered solutions tailored for real-world production environments:
- Data tokenization and detokenization capabilities for **Protegrity** and a leading North American credit reporting company
- A connector layer to integrate **CDAP** plugins into Apache Beam pipelines

By leveraging [Apache Beam](https://beam.apache.org/) and [Google Cloud Dataflow](https://cloud.google.com/products/dataflow?hl=en), Akvelon enabled its clients to achieve scalable data protection, regulatory compliance, and platform interoperability through reusable, open-source pipeline components.

## Use Case 1: Data Tokenization for Protegrity and a Leading Credit Reporting Company

### The Challenge

**Protegrity**, a leading enterprise data-security vendor, sought to enhance its data protection platform with scalable tokenization support for batch and streaming data. Their goal: allow customers such as a major North American credit reporting company to tokenize sensitive data using Google Cloud Dataflow. The solution needed to be fast, secure, reusable, and compliant with privacy regulations (e.g., HIPAA, GDPR).

### The Solution

Akvelon designed and implemented a **Dataflow Flex Template** using Apache Beam that allows users to tokenize and detokenize sensitive data within both batch and streaming pipelines.

<div class="post-scheme">
    <a href="/images/case-study/akvelon/diagram-01.png" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/akvelon/diagram-01.png" alt="Protegrity & Equifax Tokenization Pipeline">
    </a>
</div>

### Key Features
- **Seamless integration with Protegrity UDFs**, enabling native tokenization directly within Beam transforms without requiring external service orchestration
- **Support for multiple data formats** such as CSV, JSON, Parquet, allowing flexible deployment across diverse data pipelines
- **Stateful processing with `DoFn` and timers**, which improves streaming reliability and reduces overall pipeline latency
- **Full compatibility with Google Cloud Dataflow**, ensuring autoscaling, fault tolerance, and operational simplicity through managed Apache Beam execution

This design provided both Protegrity and its enterprise clients with a reusable, open-source architecture for scalable data privacy and processing.

### The Results
- **Enabled data tokenization at scale** for regulated industries
- **Accelerated adoption of Dataflow templates** across Protegrity’s customer base
- **Delivered an [open-source Flex Template](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/datatokenization/README.md)** that benefits the entire Apache Beam community

<blockquote class="case-study-quote-block case-study-quote-wrapped">
  <p class="case-study-quote-text">
    In collaboration with Akvelon, Protegrity utilized a Dataflow Flex template that helps us enable customers to tokenize and detokenize streaming and batch data from a fully managed Google Cloud Dataflow service. We appreciate Akvelon’s support as a trusted partner with Google Cloud expertise.
  </p>
  <div class="case-study-quote-author">
    <div class="case-study-quote-author-img">
        <img src="/images/case-study/akvelon/chitnis.png">
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

## Use Case 2: CDAP Connector for Apache Beam

### The Challenge

**CDAP** had extensive plugin support for Spark but lacked native compatibility with Apache Beam. This limitation prevented organizations from reusing CDAP's rich ecosystem of data connectors (e.g., Salesforce, HubSpot, ServiceNow) within Beam-based pipelines, constraining cross-platform integration.

### The Solution

Akvelon engineered a **shim layer** (CDAP Connector) that bridges CDAP plugins with Apache Beam. This innovation enables CDAP source and sink plugins to operate seamlessly within Beam pipelines.

<div class="post-scheme">
    <a href="/images/case-study/akvelon/diagram-02.png" target="_blank" title="Click to enlarge">
        <img src="/images/case-study/akvelon/diagram-02.png" alt="CDAP Connector Integration with Apache Beam">
    </a>
</div>

### Highlights

- Supports `StructuredRecord` format conversion to Beam schema (`BeamRow`)
- Enables CDAP plugins to run seamlessly in both Spark and Beam pipelines
- Facilitates integration testing across third-party data sources (e.g., Salesforce, Zendesk)
- Complies with Beam’s development and style guide for open-source contributions

The project included prototyping, test infrastructure, and Salesforce plugin pipelines to ensure robustness.

### The Results

- **Enabled seamless reuse of CDAP plugins in Beam**
  - **30+ CDAP plugins** now work seamlessly with Beam pipelines
  - **Integration time** reduced **from hours to just a few minutes**
- **Simplified execution and migration of CDAP pipelines to Beam**
  - Enabled **seamless execution** of CDAP pipelines on the Beam runtime
  - Simplified **migration of existing CDAP pipelines** to Beam with minimal changes
- **Accelerated delivery and validated performance for Google Cloud customers**
  - Delivered **rapid development cycles** with standardized plugin configurations
  - Successfully processed **5 million records** in end-to-end tests for **batch and streaming**

## Final Words

Akvelon’s contributions to Apache Beam-based solutions - from advanced tokenization for Protegrity and its enterprise customers to enabling plugin interoperability through the CDAP Connector - demonstrate the value of open-source, cloud-native data engineering. By delivering reusable and secure components, Akvelon supports enterprises in modernizing and unifying their data infrastructure.

## Watch the Solution in Action

[Architecture Walkthrough Video ](https://www.youtube.com/watch?v=IQIzdfNIAHk)

## About Akvelon, Inc.

Akvelon guides enterprises through digital transformation on Google Cloud - applying deep expertise in data engineering, AI/ML, cloud infrastructure, and custom application development to design, deploy, and scale modern workloads.

At Akvelon, we’ve built a long-standing partnership with Google Cloud - helping software-driven organizations implement, migrate, modernize, automate, and optimize their systems while making the most of cloud technologies.

As a **Google Cloud Service** and **Build Partner**, we contribute actively to the ecosystem:
- Contributing code and guidance to **Apache Beam** - including [Playground](https://github.com/apache/beam/blob/master/playground/README.md), [Tour of Beam](https://github.com/apache/beam/blob/master/learning/tour-of-beam/README.md), and the [Duet AI training set](https://github.com/apache/beam/blob/master/learning/prompts/README.md)
- Improving project infrastructure and supporting the Apache Beam community - now with an official Apache Beam Committer on our team

Backed by deep expertise in data engineering, AI/ML, cloud architecture, and application development, our engineers deliver reusable, secure, and production-ready solutions on Google Cloud for enterprises worldwide.

- [Akvelon on Google Cloud](https://cloud.google.com/find-a-partner/partner/akvelon)
- [Akvelon Data and Analytics Accelerators](https://github.com/akvelon/DnA_accelerators)

{{< case_study_feedback "Akvelon" >}}

</div>
<div class="clear-nav"></div>
