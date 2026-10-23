---
title: "Python SDK Roadmap"
aliases:
  - /roadmap/connectors-python-sdk/
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

# Python SDK Roadmap

_Last updated on Aug 2026._

## Python Version Support

Apache Beam 2.73.0 and higher support Python 3.10, 3.11, 3.12, 3.13 and 3.14. Beam aims to support new Python minor versions in a timely manner:

 - [Python Version Compatibility matrix](/documentation/sdks/python/#python-version-compatibility)

## Cross-Language Connectors and Auto-Generated Wrappers

Rather than reimplementing storage connectors natively from scratch in Python, the preferred approach for Beam data connectors is leveraging mature Java I/O connectors via Beam's cross-language (xlang) framework:

* **Preferred Cross-Language Path**: Core I/O connectors (such as Kafka, Apache Iceberg, Delta Lake, Snowflake, and JDBC) are authored as [SchemaTransforms](/documentation/sdks/python-custom-multi-language-pipelines-guide) in Java and exposed to Python pipelines. Python users can invoke them seamlessly or configure them via the simplified [Managed I/O](/documentation/io/managed-io) API.
* **Auto-Generated Transform Wrappers**: The Python SDK natively supports generating wrappers for external transforms ([design doc](https://s.apache.org/autogen-wrappers)). By discovering SchemaTransforms registered in expansion services, the Python SDK automatically provides idiomatic, typed Python wrappers and documentation without requiring developers to manually write and maintain boilerplate code.

## AI and Machine Learning (Beam ML)

Machine learning and artificial intelligence represent the primary focus area for native Python transform and connector development:

* **[RunInference](/documentation/ml/overview/)**: High-throughput, production-grade model inference supporting leading ML frameworks (including PyTorch, TensorFlow, TensorRT, ONNX, and Hugging Face) in both batch and streaming pipelines.
* **Vector Database Connectors**: Native integrations and configurations for vector databases (such as Qdrant, Pinecone, and Vertex AI Vector Search), supporting large-scale embedding generation and Retrieval-Augmented Generation (RAG) workflows.
* **LLM & Multi-Model Workflows**: Native patterns for model evaluation, multi-model ensemble pipelines, and orchestration with modern AI systems.

Learn more about Beam's machine learning capabilities in the [Beam ML Documentation](/documentation/ml/overview/).

## Contributions and Feedback

Contributions and feedback are welcome!

If you are interested in helping, you can select an unassigned issue on the Kanban board and assign it to yourself. If you cannot assign the issue to yourself, comment on the issue. When submitting a new PR, please tag [@damccorm](https://github.com/damccorm), and [@tvalentyn](https://github.com/tvalentyn).

To report a Python related issue, create a GitHub Issue in [Python label](https://github.com/apache/beam/issues?q=is%3Aissue%20state%3Aopen%20label%3Apython) and cc: [~damccorm] and [~tvalentyn] in a comment. The best way to help us identify and investigate the issue is with a minimal pipeline that reproduces the issue.

You can also discuss encountered issues on user@ or dev@ mailing lists as appropriate.
