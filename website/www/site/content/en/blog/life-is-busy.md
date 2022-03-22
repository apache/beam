---
title:  "Speaking about Beam Playground"
date:   2022-03-22 10:00:01 -0700
categories:
  - blog
authors:
  - Danielle Syse
  - Brittany Hermann
  - Pablo Estrada
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

# [PRD] Beam Playground Phase 2


## Core goals

As we take the Beam Playground forward, and make sure that it becomes a valuable, useful tool for the Beam ecosystem. To ensure value, future Beam Playground work will adhere to the following core values:



* **Intuitive:** the Playground should continue to serve its main target audience - people getting started with Beam. Feature work on it should prioritize these users, and not impede their experience.
* **Useful**: beyond first-time users, the Playground should be a tool for other Beam users to try new Beam patterns, and  reference existing ones.
* **Easy to maintain**: new features added to the Playground should not increase its maintenance burden.


# Features



1. A code editor that enables users to discover features of the Beam model
    * Beam beginner users find difficulty to learn all the possible transforms and actions of the  Beam model. The code editor should enable users to discover  Beam features, and know their mistakes/typos.
    * Principles: **Intuitive, useful**
    * Features:
        * [P0] Autocompletion for Beam symbols
        * [P0] Error highlighting
        * [P0] Per-language highlighting
        * [P2] Collapse / uncollapse code blocks
        * [P3] Auto-formatting of code
        * [P3] Multiline editing
2. Share code snippets
    * The Beam Playground should become the standard way that users share snippets of Beam code.
    * Principles: **Useful**
    * Features:
        * [P0] Generating permanent URL for code snippet
        * [P1] Embedding the Playground in my website with my code
3. Improvements to Example catalog
    * The Playground’s example catalog is extensive, but is a little hard to navigate by tag, and does not show a simple progression. The new example catalog should enable discoverability of Beam features.
    * Principles: **Useful, intuitive**
    * Features:
        * [P1] Tag-based filtering
        * [P1] Examples that advance in complexity
        * [P2] Cleanup of examples: remove examples that are not useful
4. Catalog of sources and sinks (emulated or real)
    * Beam has useful, extensive utilities and semantics for data analytics, but it is also an ETL tool. For that reason, we want Playground users to try (or at least _get a sense of_) different IO connectors for Beam.
        * **Note:** This should be implemented without adding much complexity to the Playground.
    * Principles: **Useful**
    * Features:
        * [P0] BigQueryIO reading and writing
        * [P0] PubsubIO reading
        * [P1] KafkaIO reading
        * [P2] JdbcIO reading
        * [P2] PubsubIO writing
        * [P2] KafkaIO writing
        * [P3] JdbcIO writing
5. GKE should simplify deployment
    * We currently have custom logic for App Engine to load balance our requests. GKE would simplify this.
    * Principles: **Easy to maintain, useful**
    * Features:
        * [P0] A single-command deployment and Github  Cloud Build actions to deploy
        * [P0] A deployment strategy, reads like a recipe book and defers to convention over verbose redundant documentation
        * [P1] Low-latency scaling
6. Embedded experience for website’s code snippets
    * The code snippets that exist on the website should no longer appear as static code blocks - instead, they should appear as embedded Playground windows that users can try
    * Principles: **Useful**
    * Features:
        * [P0] A fully-self contained code snippet can run on the Playground (e.g. transform catalog section)
        * [P0] If the Beam Playground is down, the code snippet should still be visible (though not runnable)
        * [P1] A code snippet that is not fully self-contained can run on the Playground (e.g. imports hidden, setup hidden).
7. A tour of Beam
    * A step-by-step website-based walkthrough of learning Beam can serve as a great on-ramp for Beam. This walkthrough should use state-of-the-art Beam features: DoFn annotations, schemas, Row-based IO, schema-aware transforms.
    * Principles: **Useful, intuitive**
    * Features:
        * [Features outlined in Mockup doc](https://docs.google.com/document/d/1sFdJdJ7U0Y6Tzw1czaP9R_7R8dhnuKUljrvwWSF0PEc/edit)


# Feature stack-rank



8. [P0] Autocompletion for Beam symbols
9. [P0] Generating permanent URL for code snippet
10. [P0] Error highlighting
11. [P0] Per-language highlighting
12. [P0] BigQueryIO reading and writing
13. [P0] PubsubIO reading
14. [P0] A fully-self contained code snippet can run on the Playground (e.g. transform catalog section)
15. [P0] If the Beam Playground is down, the code snippet should still be visible (though not runnable)
16. [P0] A single-command deployment and Github actions to deploy
17. [P0] A well-documented deployment strategy
18. [P1] Embedding the Playground in my website with my code
19. [P1] Tag-based filtering
20. [P1] Low-latency scaling
21. [P1] A code snippet that is not fully self-contained can run on the Playground (e.g. imports hidden, setup hidden).
22. [P1] KafkaIO reading
23. [P2] Cleanup of examples: remove examples that are not useful
24. [P2] JdbcIO reading
25. [P2] PubsubIO writing
26. [P2] KafkaIO writing
27. [P2] Collapse / uncollapse code blocks
28. [P3] JdbcIO writing
29. [P3] Auto-formatting of code
30. [P3] Multiline editing


# References



* [Google-Akvelon - Playground Meeting notes](https://docs.google.com/document/d/172oj39qy0jZtYF8EGt_jwFgPVgl96SvBiz2yh_3dNeI/edit?resourcekey=0-_Sl7YMQbk019PmiZPXQzfw#heading=h.15j9z4uu2cya)
* [Beam Playground Phase 2 - Akvelon features](https://docs.google.com/document/d/1VU9yX4iDrQixG3AwjR8JFo_wq1ht9aNRn3Rgy0LjA7w/edit#heading=h.yo3ds8qvs1gg)
    * [Feature: Dataset catalog](https://docs.google.com/document/d/1Ptec0o3evJlVaodFxPI3XFh-B7TtzJeCq13eMID7Chw/edit)
* [A tour of Beam - first idea doc](https://docs.google.com/document/d/1hz1V-CoiaLGFOGPCRpsfI36aVbbyiLqyARmk-B3EK68/edit?resourcekey=0-gn_ntelsh5dxWU-VN82OcQ)
* [A tour of Beam - mockup doc](https://docs.google.com/document/d/1sFdJdJ7U0Y6Tzw1czaP9R_7R8dhnuKUljrvwWSF0PEc/edit)
