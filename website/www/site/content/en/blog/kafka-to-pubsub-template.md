---
layout: post
title:  "Template to ingest data from Apache Kafka to Google Cloud Pub/Sub"
date:   2020-10-28 00:00:01 -0800
categories:
  - blog
  - java
authors:
  - Alex Kosolapov
  - Ilya Kozyrev
  - Artur Khanin
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

In this blog post, we're glad to present an example template that creates a pipeline
to read data from a single or multiple topics from
[Apache Kafka](https://kafka.apache.org/) and write data into a single topic
in [Google Pub/Sub](https://cloud.google.com/pubsub).
Such a good template doesn't only give an understanding and code examples on how to 
implement simple yet powerful pipelines, it also provides the out-of-the-box solution 
that you can just _"plug'n'play"_. 

That is why if you were looking for transferring data from Kafka into Pub/Sub - congratulations, you found it!

# Template specs

Supported data formats:
- Serializable plaintext formats, such as JSON
- [PubSubMessage](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage).

Supported input source configurations:
- Single or multiple Apache Kafka bootstrap servers
- Apache Kafka SASL/SCRAM authentication over plaintext or SSL connection
- Secrets vault service [HashiCorp Vault](https://www.vaultproject.io/).

Supported destination configuration:
- Single Google Pub/Sub topic.

In a simple scenario, the template will create an Apache Beam pipeline that will read messages from a source Kafka server with a source topic, and stream the text messages into specified Pub/Sub destination topic. Other scenarios may need Kafka SASL/SCRAM authentication, that can be performed over plain text or SSL encrypted connection. The template supports using a single Kafka user account to authenticate in the provided source Kafka servers and topics. To support SASL authenticaton over SSL the template will need an SSL certificate location and access to a secrets vault service with Kafka username and password, currently supporting HashiCorp Vault.

# Where can I run this template?

There are two ways to execute the pipeline.
1. Locally. This way has many options - run directly from your IntelliJ, or create `.jar` file and run
it in the terminal, or use your favourite method.
2. In [Google Cloud](https://cloud.google.com/). Using Google Cloud [Dataflow](https://cloud.google.com/dataflow) with `gcloud`
command-line tool you can create a Flex Template out of this Beam template and execute it in Google Cloud Platform.

The only difference between local and cloud execution is that **Flex version of this template doesn't support SSL configuration.**

# Next Steps
Don't limit yourself in playing with **the first Beam template** and diving into its implementation details!

Please
[let us know](https://beam.apache.org/community/contact-us/) if you encounter any
issues.

