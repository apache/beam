---
title:  "Apache Beam Summit 2024: Unlocking the power of ML for data processing"
date:   2024-10-16 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2024/10/16/beam-summit-2024-overview.html
authors:
  - liferoad
  - damccorm
  - rezarokni
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

At the recently concluded [Beam Summit 2024](https://beamsummit.org/), a two-day event held from September 4 to 5, numerous captivating presentations showcased the potential of Beam to address a wide range of challenges, with an emphasis on machine learning (ML). These challenges included feature engineering, data enrichment, and model inference for large-scale distributed data. In all, the summit included [47 talks](https://beamsummit.org/sessions/2024/), with 16 focused specifically on ML use cases or features and many more touching on these topics.

The talks displayed the breadth and diversity of the Beam community. Among the speakers and attendees, [23 countries](https://docs.google.com/presentation/d/1IJ1sExHzrzIFF5QXKWlcAuPdp7lKOepRQKl9BnfHxJw/edit#slide=id.g3058d3e2f5f_0_10) were represented. Attendees included Beam users, committers in the Beam project, Beam Google Summer of Code contributors, and data processing/machine learning experts.

## User-friendly turnkey transforms for ML

With the features recently added to Beam, Beam now offers a set of rich turn-key transforms for ML users that handle a wide range of ML-Ops tasks. These transforms include:

* [RunInference](https://beam.apache.org/documentation/ml/overview/#prediction-and-inference): deploy ML models on CPUs and GPUs
* [Enrichment](https://beam.apache.org/documentation/ml/overview/#data-processing): enrich data for ML feature enhancements
* [MLTransform](https://beam.apache.org/documentation/ml/overview/#data-processing): transform data into ML features

The Summit talks covering both how to use these features and how people are already using them. Highlights included:

* A talk about [scaling autonomous driving at Cruise](https://beamsummit.org/slides/2024/ScalingAutonomousDrivingwithApacheBeam.pdf)
* Multiple talks about deploying LLMs for batch and streaming inference
* Three different talks about streaming processing for [RAG](https://cloud.google.com/use-cases/retrieval-augmented-generation) (including [a talk](https://www.youtube.com/watch?v=X_VzKQOcpC4) from one of Beam's Google Summer of Code contributors\!)

## Beam YAML: Simplifying ML data processing

Beam pipeline creation can be challenging and often requires learning concepts, managing dependencies, debugging, and maintaining code for ML tasks. To simplify the entry point, [Beam YAML](https://beam.apache.org/blog/beam-yaml-release/) introduces a declarative approach that uses YAML configuration files to create data processing pipelines. No coding is required.

Beam Summit was the first opportunity that the Beam community had to show off some of the use cases of Beam YAML. It featured several talks about how Beam YAML is already a core part of many users' workflows at companies like [MavenCode](https://beamsummit.org/slides/2024/ALowCodeStructuredApproachtoDeployingApacheBeamMLWorkloadsonKubernetesusingBeamStack.pdf) and [ChartBoost](https://youtu.be/avSXvbScbW0). With Beam YAML, these companies are able to build configuration-based data processing systems, significantly lowering the bar for entry at their companies.

## Prism: Provide a unified ML pipeline development framework for local and remote runner environments

Beam provides a variety of support for portable runners, but developing a local pipeline has traditionally been painful. Local runners are often incomplete and incompatible with remote runners, such as DataflowRunner and FlinkRunner.

At Beam Summit, Beam contributors introduced [the Prism local runner](https://youtu.be/R4iNwLBa3VQ) to the community. Prism greatly improves the local developer experience and reduces the gap between local and remote execution. In particular, when handling complicated ML tasks, Prism guarantees consistent runner behavior across these runners, a task that had previously lacked consistent support.

## Summary

Beam Summit 2024 showcased the tremendous potential of Apache Beam for addressing a wide range of data processing and machine learning challenges. We look forward to seeing even more innovative use cases and contributions in the future.

To stay updated on the latest Beam developments and events, visit [the Apache Beam website](https://beam.apache.org/get-started/) and follow us on [social media](https://www.linkedin.com/company/apache-beam/). We encourage you to join [the Beam community](https://beam.apache.org/community/contact-us/) and [contribute to the project](https://beam.apache.org/contribute/). Together, let's unlock the full potential of Beam and shape the future of data processing and machine learning.