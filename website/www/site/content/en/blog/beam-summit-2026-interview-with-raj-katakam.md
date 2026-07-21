---
title: "Beam Summit 2026 | Interview with Raj Katakam, Intuit Credit Karma"
date: 2026-07-22
authors:
    - janaom
categories:
    - blog
---
<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Beam Summit 2026 | Interview with Raj Katakam, Intuit Credit Karma

Apache Beam and Google Cloud Dataflow are usually known for ETL and batch/streaming processing. [Intuit Credit Karma](https://www.creditkarma.com/) said, why stop there, and built an entire ML platform on top of them.

Here is my interview with **Raj Katakam**, Staff Machine Learning Engineer at Intuit Credit Karma. We talked about how they built a unified ML platform on Apache Beam and Dataflow to serve 140 million users, why being active in the Apache Beam community matters to him, and what advice he would give anyone just getting started with Beam and ML.

--------------

**Jana: Intuit Credit Karma’s tech stack looks like a dream! You are on Google Cloud and using services like Dataflow, BigQuery, and Managed Airflow (previously, Cloud Composer). What were the main reasons for choosing this stack, and what trade-offs did you have to consider when selecting GCP and Apache Beam/Dataflow?**

**Raj**: The selection of Google Cloud, utilizing BigQuery, Dataflow, and Managed Airflow, was a strategic decision to standardize the ML lifecycle and enable high-velocity development at scale. By leveraging these technologies, we established a unified architectural standard that allows for elastic scalability and reliability across petabyte-scale workloads. This approach ensures that the entire ML lifecycle, from initial experimentation to global production execution, is governed by a consistent framework, providing the necessary infrastructure to manage complex data movements and distributed computing requirements without sacrificing architectural integrity.

Our goal was to create a unified abstraction layer that could bridge various data processing tools like Spark and BigQuery with ML libraries such as scikit-learn and TensorFlow. GCP provided the deep, native integrations we required to smoothly transition from local prototypes to massive, distributed cloud execution.

Talking about trade-offs, the biggest hurdle was simplifying the inherent complexity of a multi-framework environment. Even with powerful connectors for GCS and BigQuery, we still had to develop Vega (Intuit Credit Karma’s internal ML platform) to shield our data scientists from manual orchestration and resource management, allowing them to remain focused on a clean, unified Python API.

**Jana: Dataflow is widely known for ETL and batch/streaming processing, but you took it a step further by using it for ML. You built Vega, a full ML explainability platform on Apache Beam and Dataflow. What motivated that choice, and what advantages did these technologies bring to the project?**

**Raj**: We utilized Dataflow as the distributed execution engine to power the broader Vega ecosystem, which serves as our comprehensive ML platform. It is important to emphasize that explainability is a foundational pillar of this ecosystem, integrated directly into the core architecture rather than treated as a standalone task. By building on Apache Beam, we ensure that every model, whether focused on feature engineering, scoring, or interpretability, benefits from a unified processing model. This integration allows us to maintain rigorous standards for model transparency and governance as a native component of our large-scale distributed ML operations.

The main motivation was to eliminate the experimentation-to-production gap. Traditional platforms forced data scientists to manually rewrite code for production frameworks, but by using Dataflow and Apache Beam as the core engine, we were able to support both batch and streaming processing alongside ML scoring within a single workflow.

The primary advantage that came out of this was the unified Python API. It allowed data scientists to build complex, end-to-end data and ML pipelines that transition seamlessly from local development on sampled data to cloud-scale production execution, without architectural changes or manual re-engineering.

**Jana: Looking back at building Vega, what were the biggest challenges you faced, and what would you do differently today?**

**Raj**: The primary challenges centered on managing the inherent complexities of distributed systems and maintaining architectural coherence across a diverse ML lifecycle. Operating at the scale of over 140 million members requires solving for high-concurrency data access, managing complex dependency graphs in distributed environments, and ensuring that performance remains consistent as workloads transition from sampled data to massive production clusters. Balancing the need for developer flexibility with the strict latency and reliability constraints of a globally distributed platform represents the most significant architectural hurdle in modern ML engineering.

Reflecting on the evolution of Vega, the biggest challenge was navigating the trade-offs of building critical infrastructure while scaling to support massive business growth with limited resources. We prioritized speed-to-market and immediate utility to enable the business, which was necessary but created technical debt in terms of documentation and onboarding. If I were starting today, I would prioritize earlier investments in developer tooling and standardized templates. We are now shifting our focus from that initial fast-and-lean build phase to long-term sustainability, refining the developer experience, automating maintenance even further, and broadening support for modern ML paradigms. This ensures that the platform remains a scalable, durable asset as it evolves to meet the next generation of business needs.

**Jana: Are there any features or improvements you would love to see in Apache Beam or Dataflow in the future?**

**Raj**: While Vega has successfully leveraged Beam for our core workflows, there are a few areas where future improvements could make a real difference. One would be enhanced data engineering primitives. We would value deeper native support for large-scale data manipulation, specifically generalized tiling operations for distributed datasets and built-in capabilities for saving and managing partial processing states. This would significantly reduce the complexity of checkpointing and state recovery in our pipelines.

The other area is real-time inference. While we have strong batch capabilities, we need more streamlined primitives in Beam to better integrate our data pipelines with real-time serving infrastructure. Reducing the complexity of the hand-off between processing and prediction would be a major win for our personalization engines.

**Jana: You are a Staff Machine Learning Engineer at Intuit Credit Karma — could you tell us a bit about your background and journey to this role? And for readers who are interested in pursuing a career in ML, what advice would you give them?**

**Raj**: As a Staff Machine Learning Engineer, my career has been defined by the practice of platform-scale engineering. I have focused on architecting ecosystems that provide high-velocity developer experiences, allowing engineering teams to move from conceptual design to production-scale deployment within a unified framework. By treating the ML lifecycle as a first-class engineering problem, we build systems that automate the complexities of infrastructure management, enabling data scientists to focus on model innovation while the platform handles the rigorous demands of distributed execution and governance.

My advice to anyone pursuing a career in ML would be to focus on first-principles thinking. Don’t just learn a specific library, understand the underlying data infrastructure, including data movement, latency constraints, and caching. The most impactful engineers I have worked with, including those I have collaborated with at Google and beyond, are the ones who can reason across domains, from low-level feature serving latency to high-level platform architecture.

**Jana: You are an active member of the Apache Beam community. This is your third Beam Summit. How important do you think community involvement is for engineers, and how has it shaped your career?**

**Raj**: Community involvement is critical. Participating in initiatives like the Apache Beam Summit has allowed me to stay at the frontier of distributed computing and directly influence the evolution of the tools we depend on. It transforms engineering from a siloed task into a collaborative effort; seeing how other engineers solve (or struggle with) similar problems helps refine your own architectural decisions and often provides the reference architectures that we would otherwise have taken years to develop independently.

**Jana: If you could give one piece of advice to someone just starting with Apache Beam, what would it be?**

**Raj**: Focus on the abstraction. Do not treat Beam as just another processing framework; treat it as the “universal language” for your data pipeline. Start by leveraging the high-level transforms and built-in IO connectors rather than trying to optimize low-level custom code immediately. Understanding how to structure your pipeline to take advantage of Beam’s windowing and triggering capabilities is what will eventually separate a good engineer from a great one when you hit the scale of millions of users.

**Jana: Thank you so much, Raj, for sharing these insights!**

**Raj**: Thank you for having me, Jana, this was a great conversation!

------------

🌐 To learn more about **Intuit Credit Karma**, visit their [website](https://www.creditkarma.com/) or check out their [LinkedIn](https://www.linkedin.com/company/intuitcreditkarma/). You can also connect with **Raj** directly on [LinkedIn](https://www.linkedin.com/in/rajkiran2190).

📌 Raj and Pallav Anand presented at Beam Summit 2026. Check out their talk, [‘Beyond the Black Box: How Intuit Credit Karma Runs ML Explainability for 140M Members with Beam’](https://beamsummit.org/sessions/2026/beyond-the-black-box-how-intuit-credit-karma-runs-ml-explainability-for-140m-members-with-beam/), and explore the full program at [beamsummit.org](https://beamsummit.org/sessions/2026/).

📺 Session recording will be available on the [Apache Beam YouTube channel](https://www.youtube.com/@ApacheBeamYT).

📰 Missed the Beam Summit 2026? Read my recap: [Beam Summit 2026: Apache Beam Just Got Even More Interesting 🐝](https://medium.com/google-cloud/beam-summit-2026-apache-beam-just-got-even-more-interesting-7c6c9967ffb9)

– [Jana Polianskaja](https://www.linkedin.com/in/jana-polianskaja/)
