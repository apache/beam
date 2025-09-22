---
title:  "Google Summer of Code 2025 - Beam YAML, Kafka and Iceberg User
Accessibility"
date:   2025-09-23 00:00:00 -0400
categories:
  - blog
  - gsoc
aliases:
  - /blog/2025/09/23/gsoc-25-yaml-ml-workflows.html
authors:
  - charlespnh

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

The relatively new Beam YAML SDK was introduced in the spirit of making data processing easy,
but it has gained little adoption for complex ML tasks and hasn’t been widely used with Managed IOs such as Kafka and Iceberg.
As part of Google Summer of Code 2025, new illustrative, production-ready pipeline examples of ML use cases with Kafka and Iceberg
data sources using the YAML SDK have been developed to address this adoption gap.

## Context
The YAML SDK was introduced in Spring 2024 as Beam’s first no-code SDK. It follows a declarative approach
of defining a data processing pipeline using a YAML DSL, as opposed to other programming language specific SDKs.
At the time, it had few meaningful examples and documentation to go along with. Key missing examples
were ML workflows and integration with the Kafka and Iceberg Managed IOs. Foundational work had already been done
to add support for ML capabilities as well as Kafka and Iceberg IO connectors in the YAML SDK, but there were no
end-to-end examples demonstrating their usage.

Beam, as well as Kafka and Iceberg, are mainstream big data technology but they also have a learning curve.
The overall theme of the project is to help democratize data processing for scientists and analysts that traditionally
don’t have a strong background in software engineering. They can now refer to these meaningful examples as the starting point,
helping them onboard faster and be more productive when authoring ML/data pipelines to their use cases with Beam and its YAML DSL.

## Contributions
The data pipelines/workflows developed are production-ready: Kafka and Iceberg data sources are set up on GCP,
and the data used are raw public datasets. The pipelines are tested end-to-end on Google Cloud Dataflow and
are also unit tested to ensure correct transformation logic.

Delivered pipelines/workflows, each with documentation as README.md, address 4 main ML use cases below:

1. **Streaming Classification Inference**: A streaming ML pipeline that demonstrates Beam YAML capability to perform
classification inference on a stream of incoming data from Kafka. The overall workflow also includes
DistilBERT model deployment and serving on Google Cloud Vertex AI where the pipeline can access for remote inferences.
The pipeline is applied to a sentiment analysis task on a stream of YouTube comments, preprocessing data and classifying
whether a comment is positive or negative. See [pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/ml/sentiment_analysis/streaming_sentiment_analysis.yaml) and [documentation](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml/examples/transforms/ml/sentiment_analysis).


2. **Streaming Regression Inference**: A streaming ML pipeline that demonstrates Beam YAML capability to perform
regression inference on a stream of incoming data from Kafka. The overall workflow also includes
custom model training, deployment and serving on Google Cloud Vertex AI where the pipeline can access for remote inferences.
The pipeline is applied to a regression task on a stream of taxi rides, preprocessing data and predicting the fare amount
for every ride. See [pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/ml/taxi_fare/streaming_taxifare_prediction.yaml) and [documentation](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml/examples/transforms/ml/taxi_fare).


3. **Batch Anomaly Detection**: A ML workflow that demonstrates ML-specific transformations
and reading from/writing to Iceberg IO. The workflow contains unsupervised model training and several pipelines that leverage
Iceberg for storing results, BigQuery for storing vector embeddings and MLTransform for computing embeddings to demonstrate
an end-to-end anomaly detection workflow on a dataset of system logs. See [workflow](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/ml/log_analysis/batch_log_analysis.sh) and [documentation](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml/examples/transforms/ml/log_analysis).


4. **Feature Engineering & Model Evaluation**: A ML workflow that demonstrates Beam YAML capability to do feature engineering
which is subsequently used for model evaluation, and its integration with Iceberg IO. The workflow contains model training
and several pipelines, showcasing an end-to-end Fraud Detection MLOps solution that generates features and evaluates models
to detect credit card transaction frauds. See [workflow](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/ml/fraud_detection/fraud_detection_mlops_beam_yaml_sdk.ipynb) and [documentation](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml/examples/transforms/ml/fraud_detection).

## Challenges
The main challenge of the project has been a lack of previous YAML pipeline examples and good documentation to rely on.
Unlike the Python or Java SDKs where there are already many notebooks and end-to-end examples demonstrating various use cases,
the examples for YAML SDKs only involved simple transformations such as filter, group by, etc…

Another challenge is writing unit tests for the pipeline to ensure that the pipeline’s logic is correct.
It is a learning curve to understand how the existing test suite is set up and how it can be used to write unit tests for
the data pipelines.

## Conclusion & Personal Thoughts
These production-ready pipelines demonstrate the potential of Beam YAML SDK to author complex ML workflows
that interact with Iceberg and Kafka. The examples are a nice addition to Beam, especially with Beam 3.0.0 milestones
coming up where low-code/no-code, ML capabilities and Managed IOs are focused on.

I had an amazing time working with the big data technology Beam, Iceberg and Kafka as well as many Google Cloud services
(Dataflow, Vertex AI and Google Kubernetes Engine, to name a few). I’ve always wanted to work more in the ML space, and this
experience has been a great growth opportunity for me. Google Summer of Code this year has been selective, and the project's success
would not have been possible without the support of my mentor, Chamikara Jayalath. It's been a pleasure working closely
with him and the broader Beam community to contribute to this open-source project that has a meaningful impact on the
data engineering community.

My advice for future Google Summer of Code participants is to first and foremost research and choose a project that align closely
with your interest. Most importantly, spend a lot of time making yourself visible and writing a good proposal when the program
is opened for applications. Being visible (e.g. by sharing your proposal, or generally any ideas and questions on the project's
communication channel early on) makes it more likely for you to be selected; and a good proposal not only will make you even
more likely to be in the program, but also give you a lot of confident when contributing and completing the project.

## References
- [Google Summer of Code Project Listing](https://summerofcode.withgoogle.com/programs/2025/projects/f4kiDdus)
- [Google Summer of Code Final Report](https://docs.google.com/document/d/1MSAVF6X9ggtVZbqz8YJGmMgkolR_dve0Lr930cByyac/edit?usp=sharing)
