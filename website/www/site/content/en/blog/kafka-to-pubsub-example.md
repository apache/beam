---
layout: post
title:  "Example to ingest data from Apache Kafka to Google Cloud Pub/Sub"
date:   2021-01-15 00:00:01 -0800
categories:
  - blog
  - java
authors:
  - arturkhanin
  - ilyakozyrev
  - alexkosolapov
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

In this blog post we present an example that creates a pipeline to read data from a single topic or
multiple topics from [Apache Kafka](https://kafka.apache.org/) and write data into a topic
in [Google Pub/Sub](https://cloud.google.com/pubsub). The example provides code samples to implement
simple yet powerful pipelines and also provides an out-of-the-box solution that you can just _"
plug'n'play"_.

This end-to-end example is included
in [Apache Beam release 2.27](/blog/beam-2.27.0/)
and can be downloaded [here](/get-started/downloads/#2270-2020-12-22).

We hope you will find this example useful for setting up data pipelines between Kafka and Pub/Sub.

# Example specs

Supported data formats:

- Serializable plain text formats, such as JSON
- [PubSubMessage](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage)

Supported input source configurations:

- Single or multiple Apache Kafka bootstrap servers
- Apache Kafka SASL/SCRAM authentication over plaintext or SSL connection
- Secrets vault service [HashiCorp Vault](https://www.vaultproject.io/)

Supported destination configuration:

- Single Google Pub/Sub topic

In a simple scenario, the example will create an Apache Beam pipeline that will read messages from a
source Kafka server with a source topic, and stream the text messages into specified Pub/Sub
destination topic. Other scenarios may need Kafka SASL/SCRAM authentication, that can be performed
over plaintext or SSL encrypted connection. The example supports using a single Kafka user account
to authenticate in the provided source Kafka servers and topics. To support SASL authentication over
SSL the example will need an SSL certificate location and access to a secrets vault service with
Kafka username and password, currently supporting HashiCorp Vault.

# Where can I run this example?

There are two ways to execute the pipeline.

1. Locally. This way has many options - run directly from your IntelliJ, or create `.jar` file and
   run it in the terminal, or use your favourite method of running Beam pipelines.
2. In [Google Cloud](https://cloud.google.com/) using Google
   Cloud [Dataflow](https://cloud.google.com/dataflow):
    - With `gcloud` command-line tool you can create
      a [Flex Template](https://cloud.google.com/dataflow/docs/concepts/dataflow-templates)
      out of this Beam example and execute it in Google Cloud Platform. _This requires corresponding
      modifications of the example to turn it into a template._
    - This example exists as
      a [Flex Template version](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/master/v2/kafka-to-pubsub)
      within [Google Cloud Dataflow Template Pipelines](https://github.com/GoogleCloudPlatform/DataflowTemplates)
      repository and can be run with no additional code modifications.

# Next Steps

Give this **Beam end-to-end example** a try. If you are new to Beam, we hope this example will give
you more understanding on how pipelines work and look like. If you are already using Beam, we hope
some code samples in it will be useful for your use cases.

Please
[let us know](/community/contact-us/) if you encounter any issues.

