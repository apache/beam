---
title: "My Experience at Beam College 2025: 3rd Place Hackathon Winner"
date: 2025-06-16
authors:
  - msugar
categories:
  - blog
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

## Introduction: The Spark of an Idea

In 2025, I had the opportunity to participate in the [Beam College Hackathon](https://beamcollege.dev/hackathon/), a fantastic event that brings together students and professionals to explore the power of Apache Beam.

For my project, I built **[Anomaflow](https://github.com/msugar/anomaflow)**, an anomaly detection pipeline using **Apache Beam** and **Google Cloud Dataflow**. It was my first public hackathon, and the experience was both rewarding and creatively energizing. I’m proud to share that Anomaflow earned **3rd place** in the competition.

## The Project: Building Anomaflow

### Goal

The primary objective of Anomaflow was to process **host telemetry data** from systems monitored by OpenTelemetry agents. The long-term goal is to detect anomalies in near real time, which has important applications in cybersecurity.

### Tech Stack

Here’s a breakdown of the technologies used:

- **Apache Beam (Python SDK)** for pipeline development
- **Bindplane Collector & Server** for OpenTelemetry data collection and configuration
- **Google Compute Engine (GCE)** instancess for hosting the Bindplane Server and Collector
- **Google Cloud Dataflow** for running the Beam pipeline at scale
- **Google Cloud Storage (GCS)** as the pipeline’s source and sink during the hackathon
- **Terraform** to provision GCP infrastructure
- **Docker** for packaging and deployment

## The Hackathon Journey: From Streaming Vision to Batch Reality

### The Initial Vision

The original plan was to create a fully streaming pipeline: the **Bindplane Collector** running on **GCE** would upload telemetry files to **GCS**, which would trigger **notifications via Pub/Sub**. These notifications would then initiate processing in a **Beam pipeline**, with enriched results written to **BigQuery** for analysis and visualization.

### The Pivot

However, working solo during a time-limited hackathon meant I had to be pragmatic. I decided to implement a **batch pipeline** instead, reading from and writing to **GCS buckets**. This allowed me to deliver a functional MVP while preserving a foundation that can evolve toward the original streaming vision.

### Key Learnings

Although I already had some real-world experience with Apache Beam, the hackathon gave me the freedom to explore new patterns and tools in a low-risk environment. It was refreshing to iterate rapidly, test ideas, and push beyond my daily work scope.

## What’s Next for Anomaflow?

Anomaflow is just getting started.

I plan to evolve the pipeline into a true **streaming system** using **Pub/Sub** and **BigQuery**. I also want to explore **sliding windows**, **custom anomaly detection models**, and **alerting mechanisms**. With its modular design and strong foundation in Beam, Anomaflow will serve as a base for several future cybersecurity analytics tools I have in mind.

**Watch the demo**: [https://www.youtube.com/watch?v=dpbOm5ekOTc](https://www.youtube.com/watch?v=dpbOm5ekOTc)

## Tips for Future Participants

If you’re considering joining Beam College next year:

- **Use the mentors!** The Beam community professionals volunteering their time are a valuable resource. Ask questions, get feedback.
- **Check out the [Beam learning resources](https://beam.apache.org/get-started/resources/learning-resources/)**. They’re super helpful, especially for getting started with the Beam model and runners.

## Conclusion

I’m currently a **Software Architect and Data Engineer at TELUS Security**, where I work on the **Cybersecurity Analytics and Software Engineering** team. We design data pipelines to detect and respond to threats at scale.

Participating in Beam College was a great way to stretch my skills, meet passionate Beam users, and contribute to a vibrant open source community. I’m excited to see what others will build in future editions!

– [Marcio Sugar](https://www.linkedin.com/in/marcio-sugar/)
