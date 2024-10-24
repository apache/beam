---
title: "Learning Resources"
aliases:
- /get-started/resources/learning-resources/
- /documentation/resources/learning-resources/
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

# Learning Resources

Welcome to our learning resources. This page contains a collection of resources that will help you to get started and use Apache Beam. If you’re just starting, you can view this as a guided tour, otherwise you can jump straight to any section of your interest.

If you have additional material that you would like to see here, please let us know at [user@beam.apache.org](mailto:user@beam.apache.org)!

{{< toc >}}

## Getting Started {#getting-started}

### Quickstart

*   **[Java Quickstart](/get-started/quickstart-java/)** - How to set up and run a WordCount pipeline on the Java SDK.
*   **[Python Quickstart](/get-started/quickstart-py/)** - How to set up and run a WordCount pipeline on the Python SDK.
*   **[Go Quickstart](/get-started/quickstart-go/)** - How to set up and run a WordCount pipeline on the Go SDK.
*   **[Java Development Environment](https://medium.com/google-cloud/setting-up-a-java-development-environment-for-apache-beam-on-google-cloud-platform-ec0c6c9fbb39)** - Setting up a Java development environment for Apache Beam using IntelliJ and Maven.
*   **[Python Development Environment](https://medium.com/google-cloud/python-development-environments-for-apache-beam-on-google-cloud-platform-b6f276b344df)** - Setting up a Python development environment for Apache Beam using PyCharm.

### Learning the Basics

*   **[WordCount](/get-started/wordcount-example/)** - Walks you through the code of a simple WordCount pipeline. This is a very basic pipeline intended to show the most basic concepts of data processing. WordCount is the "Hello World" for data processing.
*   **[Mobile Gaming](/get-started/mobile-gaming-example/)** - Introduces how to consider time while processing data, user defined transforms, windowing, filtering data, streaming pipelines, triggers, and session analysis. This is a great place to start once you get the hang of WordCount.

### Fundamentals

*   **[Programming Guide](/documentation/programming-guide/)** - The Programming Guide contains more in-depth information on most topics in the Apache Beam SDK. These include descriptions on how everything works as well as code snippets to see how to use every part. This can be used as a reference guidebook.
*   **[The world beyond batch: Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101)** - Covers some basic background information, terminology, time domains, batch processing, and streaming.
*   **[The world beyond batch: Streaming 102](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102)** - Tour of the unified batch and streaming programming model in Beam, alongside with an example to explain many of the concepts.
*   **[Apache Beam Execution Model](/documentation/runtime/model)** - Explanation on how runners execute an Apache Beam pipeline. This includes why serialization is important, and how a runner might distribute the work in parallel to multiple machines.

### Common Patterns

*   **[Common Use Case Patterns Part 1](https://cloud.google.com/blog/products/gcp/guide-to-common-cloud-dataflow-use-case-patterns-part-1)** - Common patterns such as writing data to multiple storage locations, slowly-changing lookup cache, calling external services, dealing with bad data, and starting jobs through a REST endpoint.
*   **[Common Use Case Patterns Part 2](https://cloud.google.com/blog/products/gcp/guide-to-common-cloud-dataflow-use-case-patterns-part-2)** - Common patterns such as GroupBy using multiple data properties, joining two PCollections on a common key, streaming large lookup tables, merging two streams with different window lengths, and threshold detection with time-series data.
*   **[Retry Policy](https://nanthrax.blogspot.com/2018/02/apache-beam-easily-implement-backoff_18.html)** - Adding a retry policy to a `DoFn`.

## Articles {#articles}

### Data Analysis

*   **[Predicting news social engagement](https://medium.com/google-cloud/predicting-social-engagement-for-the-worlds-news-with-tensorflow-and-cloud-dataflow-part-1-b92ba8f14a7)** - Using multiple data sources, many common design patterns, and sentiment analysis to get insights into different news articles for TensorFlow and Dataflow.
*   **[Processing IoT Data](https://cloud.google.com/community/tutorials/cloud-iot-rtdp)** - IoT sensors are continuously streaming data to the cloud. Learn how to handle the sensor data which can be useful for real-time monitoring, alerts, long-term data storage for analysis, performance improvement, and model training.

### Data Migration

*   **[Oracle Database to Google BigQuery](https://medium.com/google-cloud/oracle-data-to-google-bigquery-using-google-cloud-dataflow-and-dataprep-20884571a9e5)** - Migrate data from an [Oracle Database](https://www.oracle.com/database/index.html) into [BigQuery](https://cloud.google.com/bigquery) using [Dataprep](https://cloud.google.com/dataprep/).
*   **[Google BigQuery to Google Datastore](https://medium.com/google-cloud/export-bigquery-to-google-datastore-with-apache-beam-google-dataflow-7fff1566f345)** - Migrate data from a [BigQuery](https://cloud.google.com/bigquery/) table into [Datastore](https://cloud.google.com/datastore/) without thinking of its schema.
*   **[SAP HANA to Google BigQuery](https://cloud.google.com/blog/products/gcp/using-apache-beam-and-cloud-dataflow-to-integrate-sap-hana-and-bigquery)** - Migrate data from a [SAP HANA](https://www.sapphiresystems.com/en-us/products/sap-hana) in-memory database into [BigQuery](https://cloud.google.com/bigquery).

### Machine Learning

*   **[Machine Learning using the RunInference API](/documentation/ml/about-ml)** - Use Apache Beam with the RunInference API to use machine learning (ML) models to do local and remote inference with batch and streaming pipelines. Follow the [RunInference API pipeline examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference) to do image classification, image segmentation, language modeling, and MNIST digit classification. See examples of [RunInference transforms](/documentation/transforms/python/elementwise/runinference/).
*   **[Machine Learning Preprocessing and Prediction](https://cloud.google.com/dataflow/examples/molecules-walkthrough)** - Predict the molecular energy from data stored in the [Spatial Data File](https://en.wikipedia.org/wiki/Spatial_Data_File) (SDF) format. Train a [TensorFlow](https://www.tensorflow.org/) model with [tf.Transform](https://github.com/tensorflow/transform) for preprocessing in Python. This also shows how to create batch and streaming prediction pipelines in Apache Beam.
*   **[Machine Learning Preprocessing](https://cloud.google.com/blog/products/ai-machine-learning/pre-processing-tensorflow-pipelines-tftransform-google-cloud)** - Find the optimal parameter settings for simulated physical machines like a bottle filler or cookie machine. The goal of each simulated machine is to have the same input/output of the actual machine, making it a "digital twin". This uses [tf.Transform](https://github.com/tensorflow/transform) for preprocessing.

### Advanced Concepts

*   **[Running on AppEngine](https://amygdala.github.io/dataflow/app_engine/2017/10/24/gae_dataflow.html)** - Use a Dataflow template to launch a pipeline from Google AppEngine, and how to run the pipeline periodically via a cron job.
*   **[Stateful Processing](/blog/2017/02/13/stateful-processing.html)** - Learn how to access a persistent mutable state while processing input elements, this allows for _side effects_ in a `DoFn`. This can be used for arbitrary-but-consistent index assignment, if you want to assign a unique incrementing index to each incoming element where order doesn't matter.
*   **[Timely and Stateful Processing](/blog/2017/08/28/timely-processing.html)** - An example on how to do batched RPC calls. The call requests are stored in a mutable state as they are received. Once there are either enough requests or a certain time has passed, the batch of requests is triggered to be sent.
*   **[Running External Libraries](https://cloud.google.com/blog/products/gcp/running-external-libraries-with-cloud-dataflow-for-grid-computing-workloads)** - Call an external library written in a language that does not have a native SDK in Apache Beam such as C++.

## Videos {#videos}

*  **[Getting Started with Apache Beam](https://www.youtube.com/playlist?list=PLIivdWyY5sqIEiHGunZXg_yoS7unlHNJt)** - Five part video series for understanding basic to advanced concepts.
*  See more [Videos and Podcasts](/get-started/resources/videos-and-podcasts/)

## Courses {#courses}

*  **[Beam College](https://beamcollege.dev/)** -- Free live and recorded lessons for learning Beam and data processing.
*  **[Serverless Data Processing](https://www.coursera.org/specializations/serverless-data-processing-with-dataflow)** - Course specialized for Dataflow runner.

## Books {#books}

### Building Big Data Pipelines with Apache Beam

**[Building Big Data Pipelines with Apache Beam](https://www.packtpub.com/product/building-big-data-pipelines-with-apache-beam/9781800564930)** by Jan Lukavský, Packt. (January 2022). A general description of the Apache Beam model including gradually built examples that help create solid understanding of the subject. In the first part the book explains concepts using Java SDK, then SQL DSL and Portability layer with focus on Python SDK. The last part of the book is dedicated to more advanced topics like IO connectors using Splittable DoFn and description of how a typical runner executes Pipeline.

###  Streaming Systems: The What, Where, When, and How of Large-Scale Data Processing

**[Streaming Systems: The What, Where, When, and How of Large-Scale Data Processing](https://learning.oreilly.com/library/view/streaming-systems/9781491983867/)** by Tyler Akidau, Slava Chernyak, Reuven Lax. (August 2018). Expanded from Tyler Akidau’s popular blog posts "Streaming 101" and "Streaming 102", this book takes you from an introductory level to a nuanced understanding of the what, where, when, and how of processing real-time data streams.


## Certifications {#certifications}

### Getting Started with Apache Beam Quest

**[Get Started with Apache Beam](https://www.cloudskillsboost.google/course_templates/724)**  This quest includes four labs that teach you how to write and test Apache Beam pipelines. Three of the labs use Java and one uses Python. Each lab takes about 1.5 hours to complete. When you complete the quest, you're granted a badge that you can use to show your Beam expertise.


## Interactive Labs {#interactive-labs}

### Java

*   **[Big Data Text Processing Pipeline](https://qwiklabs.com/focuses/608?locale=en&parent=catalog)** (40m) - Run a word count pipeline on the Dataflow runner.
*   **[Real Time Machine Learning](https://qwiklabs.com/focuses/3393?locale=en&parent=catalog)** (45m) - Create a real-time flight delay prediction service using historical data on internal flights in the United States.
*   **[Visualize Real-Time Geospatial Data](https://qwiklabs.com/focuses/1160?locale=en&parent=catalog)** (60m) - Process real-time streaming data from a real-time real world historical data set, store the results in BigQuery, and visualize the geospatial data on Data Studio.
*   **[Processing Time Windowed Data](https://qwiklabs.com/focuses/3392?locale=en&parent=catalog)** (90m) - Implement time-windowed aggregation to augment the raw data in order to produce a consistent training and test datasets for a machine learning model.

### Python

*   **[Python Qwik Start](https://www.qwiklabs.com/focuses/1098?parent=catalog)** (30m) - Run a word count pipeline on the Dataflow runner.
*   **[Simulate historic flights](https://qwiklabs.com/focuses/1159?locale=en&parent=catalog)** (60m) - Simulate real-time historic internal flights in the United States and store the resulting simulated data in BigQuery.

## Beam Katas {#beam-katas}

Beam Katas are interactive Beam coding exercises (i.e. [code katas](http://codekata.com/))
that can help you to learn Apache Beam concepts and programming model hands-on.
Built based on [JetBrains Educational Products](https://www.jetbrains.com/education/), Beam Katas
objective is to provide a series of structured hands-on learning experiences for learners
to understand about Apache Beam and its SDKs by solving exercises with gradually increasing
complexity. Beam Katas are available for both Java and Python SDKs.

### Java

*   Download [IntelliJ Edu](https://www.jetbrains.com/education/download/#section=idea)
*   Upon opening the IDE, expand the "Learn and Teach" menu, then select "Browse Courses"
*   Search for "Beam Katas - Java"
*   Expand the "Advanced Settings" and modify the "Location" and "Jdk" appropriately
*   Click "Join"
*   [Learn more](https://www.jetbrains.com/help/education/learner-start-guide.html?section=Introduction%20to%20Java#explore_course) about how to use the Education product

### Python

*   Download [PyCharm Edu](https://www.jetbrains.com/education/download/#section=pycharm-edu)
*   Upon opening the IDE, expand the "Learn and Teach" menu, then select "Browse Courses"
*   Search for "Beam Katas - Python"
*   Expand the "Advanced Settings" and modify the "Location" and "Interpreter" appropriately
*   Click "Join"
*   [Learn more](https://www.jetbrains.com/help/education/learner-start-guide.html?section=Introduction%20to%20Python#explore_course) about how to use the Education product

## Code Examples {#code-examples}

### Dataflow Cookbook

The [cookbook](https://github.com/GoogleCloudPlatform/dataflow-cookbook) includes examples in Java, Python, and Scala (via Scio), provides ready-to-launch and self-contained Beam pipelines.

### Java

*   **[Snippets 1](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/cookbook)** - Commonly-used data analysis patterns such as how to use [BigQuery](https://cloud.google.com/bigquery), a CombinePerKey transform, remove duplicate lines in files, filtering, joining PCollections, getting the maximum value of a PCollection, etc.
*   **[Snippets 2](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/common)** - Additional examples on common tasks such as configuring [BigQuery](https://cloud.google.com/bigquery), [PubSub](https://cloud.google.com/pubsub/), writing one file per window, etc.
*   **[Complete Examples](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples/complete)** - End-to-end example pipelines such as an auto complete, a streaming word extract, calculating the Term Frequency-Inverse Document Frequency ([TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)), getting the top Wikipedia sessions, traffic max lane flow, traffic routes, etc.
*   **[Pub/Sub to BigQuery](https://github.com/GoogleCloudPlatform/cloud-code-samples/tree/v1/java/java-dataflow-samples/read-pubsub-write-bigquery)** - A complete example demonstrates using Apache Beam on Dataflow to convert JSON encoded Pub/Sub subscription message strings into structured data and write that data to a BigQuery table.

### Python

*   **[Snippets](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/cookbook)** - Commonly-used data analysis patterns such as how to use [BigQuery](https://cloud.google.com/bigquery), [Datastore](https://cloud.google.com/datastore/), coders, combiners, filters, custom PTransforms, etc.
*   **[Complete Examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/complete)** - End-to-end example pipelines such as an auto complete, getting mobile gaming statistics, calculating the [Julia set](https://en.wikipedia.org/wiki/Julia_set), solving distributing optimization tasks, estimating PI, calculating the Term Frequency-Inverse Document Frequency ([TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)), getting the top Wikipedia sessions, etc.

### Beam Playground

* [Beam Playground](https://play.beam.apache.org) is an interactive environment to try out Beam transforms and examples without having to install Apache Beam in your environment.
    You can try the available Apache Beam examples at [Beam Playground](https://play.beam.apache.org).
* Learn more about how to add an Apache Beam example/test/kata into Beam Playground catalog [here](/get-started/try-beam-playground/#how-to-add-new-examples).

## API Reference {#api-reference}

*   **[Java API Reference](/documentation/sdks/javadoc/)** - Official API Reference for the Java SDK.
*   **[Python API Reference](/documentation/sdks/pydoc/)** - Official API Reference for the Python SDK.
*   **[Go API Reference](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam)** - Official API Reference for the Go SDK.

## Feedback and Suggestions {#feedback-and-suggestions}

We are open for feedback and suggestions, you can find different ways to reach out to the community in the [Contact Us](/community/contact-us/) page.

If you have a bug report or want to suggest a new feature, you can let us know by [submitting a new issue](https://github.com/apache/beam/issues/new/choose).

## How to Contribute {#how-to-contribute}

We welcome contributions from everyone! To learn more on how to contribute, check our [Contribution Guide](/contribute/).
