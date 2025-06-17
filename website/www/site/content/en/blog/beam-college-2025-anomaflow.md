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

In the 2025 edition of the [Beam College Hackathon](https://beamcollege.dev/hackathon/), I had the opportunity to work on a personal project I called [**Anomaflow**](https://github.com/msugar/anomaflow), an anomaly detection pipeline using **Apache Beam** and **Google Cloud Dataflow**.

The goal was to build a data pipeline capable of processing host telemetry data collected by **Bindplane Collectors**, which are built on [**OpenTelemetry**](https://opentelemetry.io/). I used **Terraform** to provision all the GCP infrastructure and **Python** for the code, packaging it with **Docker** for deployment.

Initially, I wanted to build a fully streaming solution where the pipeline would be triggered by **GCS notifications published to Pub/Sub**, with output written to a **BigQuery** table. But since I was working solo, I had to scope it down to a **batch pipeline** that reads from and writes to a **GCS bucket**.

Although I already had some real-life experience using Apache Beam, the hackathon gave me the perfect excuse to experiment freely and iterate quickly. I plan to continue developing Anomaflow to reach my original vision, and even use it as the foundation for new ideas I’ve been exploring around telemetry processing and streaming analytics in cybersecurity.

### Tips for Future Participants

If you’re considering joining Beam College next year:

- **Use the mentors!** The Beam community professionals volunteering their time are a valuable resource. Ask questions, get feedback.
- **Check out the [Beam learning resources](https://beam.apache.org/get-started/resources/learning-resources/)**. They’re super helpful, especially for getting started with the Beam model and runners.

---

I'm currently a **Software Architect and Data Engineer at TELUS**, where I work in the **Cybersecurity Analytics and Software Engineering** Team, building data pipelines and systems to analyze threats at scale. Participating in Beam College was a fun way to stretch my skills, meet fellow Beam enthusiasts, and contribute to the community.

Looking forward to seeing what others build next year!

– [Marcio Sugar](https://www.linkedin.com/in/marcio-sugar/)

