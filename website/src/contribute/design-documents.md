---
layout: section
title: 'Beam Design Documents'
section_menu: section-menu/contribute.html
permalink: /contribute/design-documents/
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

# Design Documents
This is a collection of documents that may or may not be up to date.

## Documents by category
### Project Incubation (2016)
- Technical Vision [[doc](https://docs.google.com/document/d/1UyAeugHxZmVlQ5cEWo_eOPgXNQA1oD-rGooWOSwAqh8/edit)], [[slides](https://docs.google.com/presentation/d/1E9seGPB_VXtY_KZP4HngDPTbsu5RVZFFaTlwEYa88Zw)]
- Repository Structure [[doc](https://docs.google.com/document/d/1mTeZED33Famq25XedbKeDlGIJRvtzCXjSfwH9NKQYUE)]
- Flink runner: Current status and development roadmap [[doc](https://docs.google.com/document/d/1QM_X70VvxWksAQ5C114MoAKb1d9Vzl2dLxEZM4WYogo)]
- Spark Runner Technical Vision [[doc](https://docs.google.com/document/d/1y4qlQinjjrusGWlgq-mYmbxRW2z7-_X5Xax-GG0YsC0)]
- PPMC deep dive [[slides](https://docs.google.com/presentation/d/1uTb7dx4-Y2OM_B0_3XF_whwAL2FlDTTuq2QzP9sJ4Mg)]

### Beam Model
- Checkpoints [[doc](https://s.apache.org/FIWQ)]
- A New DoFn [[doc](https://s.apache.org/a-new-dofn)], [[slides](https://s.apache.org/presenting-a-new-dofn)]
- Proposed Splittable DoFn API changes [[doc](https://docs.google.com/document/d/1BGc8pM1GOvZhwR9SARSVte-20XEoBUxrGJ5gTWXdv3c)]
- Splittable DoFn (Obsoletes Source API) [[doc](http://s.apache.org/splittable-do-fn)]
  - Reimplementing Beam API classes on top of Splittable DoFn on top of Source API [[doc](https://s.apache.org/sdf-via-source)]
  - New TextIO features based on SDF [[doc](http://s.apache.org/textio-sdf)]
  - Watch transform [[doc](http://s.apache.org/beam-watch-transform)]
  - Bundles w/ SplittableDoFns [[doc](https://s.apache.org/beam-bundles-backlog-splitting)]
- State and Timers for DoFn [[doc](https://s.apache.org/beam-state)]
- ContextFn [[doc](http://s.apache.org/context-fn)]
- Static Display Data [[doc](https://docs.google.com/document/d/11enEB9JwVp6vO0uOYYTMYTGkr3TdNfELwWqoiUg5ZxM)]
- Lateness (and Panes) in Apache Beam [[doc](https://s.apache.org/beam-lateness)]
- Triggers in Apache Beam [[doc](https://s.apache.org/beam-triggers)]
- Triggering is for sinks [[doc](https://s.apache.org/beam-sink-triggers)] (not implemented)
- Pipeline Drain [[doc](https://docs.google.com/document/d/1NExwHlj-2q2WUGhSO4jTu8XGhDPmm3cllSN8IMmWci8)]
- Pipelines Considered Harmful [[doc](https://s.apache.org/no-beam-pipeline)]
- Side-Channel Inputs [[doc](https://docs.google.com/document/d/1e_-MenoW2cQ-6-EGVVqfOR-B9FovVXqXyUm4-ZwlgKA)]
- Dynamic Pipeline Options [[doc](https://docs.google.com/document/d/1I-iIgWDYasb7ZmXbGBHdok_IK1r1YAJ90JG5Fz0_28o)]
- SDK Support for Reading Dynamic PipelineOptions [[doc](https://docs.google.com/document/d/17I7HeNQmiIfOJi0aI70tgGMMkOSgGi8ZUH-MOnFatZ8)]
- Fine-grained Resource Configuration in Beam [[doc](https://docs.google.com/document/d/1N0y64dbzmukLLEy6M9CygdI_H88pIS3NtcOAkL5-oVw)]
- External Join with KV Stores [[doc](https://docs.google.com/document/d/1B-XnUwXh64lbswRieckU0BxtygSV58hysqZbpZmk03A)]
- Error Reporting Callback (WIP) [[doc](https://docs.google.com/document/d/1o2VXwCL97k3G-1BR9RSKNc6XtJTIA6SEKPMne91S67Y)]
- Snapshotting and Updating Beam Pipelines [[doc](https://docs.google.com/document/d/1UWhnYPgui0gUYOsuGcCjLuoOUlGA4QaY91n8p3wz9MY)]
- Requiring PTransform to set a coder on its resulting collections [[mail](https://lists.apache.org/thread.html/1dde0b5a93c2983cbab5f68ce7c74580102f5bb2baaa816585d7eabb@%3Cdev.beam.apache.org%3E)]
- Support of @RequiresStableInput annotation [[doc](https://docs.google.com/document/d/117yRKbbcEdm3eIKB_26BHOJGmHSZl1YNoF0RqWGtqAM)], [[mail](https://lists.apache.org/thread.html/ae3c838df060e47148439d1dad818d5e927b2a25ff00cc4153221dff@%3Cdev.beam.apache.org%3E)]
- [PROPOSAL] @onwindowexpiration [[mail](https://lists.apache.org/thread.html/1dab7f17c97378e665928b11116cbd887dc7be93390ab26c593ee49a@%3Cdev.beam.apache.org%3E)]
- AutoValue Coding and Row Support [[doc](https://docs.google.com/document/d/1ucoik4WzUDfilqIz3I1AuMHc1J8DE6iv7gaUCDI42BI)] 

### IO / Filesystem
- IOChannelFactory Redesign [[doc](https://docs.google.com/document/d/11TdPyZ9_zmjokhNWM3Id-XJsVG3qel2lhdKTknmZ_7M)]
- Configurable BeamFileSystem [[doc](https://docs.google.com/document/d/1-7vo9nLRsEEzDGnb562PuL4q9mUiq_ZVpCAiyyJw8p8)]
- New API for writing files in Beam [[doc](http://s.apache.org/fileio-write)]
- Dynamic file-based sinks [[doc](https://docs.google.com/document/d/1Bd9mJO1YC8vOoFObJFupVURBMCl7jWt6hOgw6ClwxE4)]
- Event Time and Watermarks in KafkaIO [[doc](https://docs.google.com/document/d/1DyWcLJpALRoUfvYUbiPCDVikYb_Xz2X7Co2aDUVVd4I)]
- Exactly-once Kafka sink [[doc](https://lists.apache.org/thread.html/fb394e576e6e858205307b033c5a5c6cc3923a17606814a54036c570@%3Cdev.beam.apache.org%3E)]

### Metrics
- Get Metrics API: Metric Extraction via proto RPC API. [[doc](https://s.apache.org/get-metrics-api)]
- Metrics API [[doc](http://s.apache.org/beam-metrics-api)]
- I/O Metrics [[doc](https://s.apache.org/standard-io-metrics)]
- Metrics extraction independent from runners / execution engines [[doc](https://s.apache.org/runner_independent_metrics_extraction)]
- Watermark Metrics [[doc](https://docs.google.com/document/d/1ykjjG97DjVQP73jGbotGRbtK38hGvFbokNEOuNO4DAo)]
- Support Dropwizard Metrics in Beam [[doc](https://docs.google.com/document/d/1-35iyCIJ9P4EQONlakgXBFRGUYoOLanq2Uf2sw5EjJw)]

### Runners
- Runner Authoring Guide [[doc](https://s.apache.org/beam-runner-guide)] (obsoletes [[doc](http://s.apache.org/beam-runner-api)] and [[doc](https://s.apache.org/beam-runner-1-pager)])
- Composite PInputs, POutputs, and the Runner API [[doc](https://s.apache.org/beam-runner-composites)]
- Side Input Architecture for Apache Beam [[doc](https://s.apache.org/beam-side-inputs-1-pager)]
- Runner supported features plugin [[doc](https://s.apache.org/k79W)]

### SQL / Schema
- Streams and Tables [[doc](https://s.apache.org/beam-streams-tables)]
- Streaming SQL [[doc](http://s.apache.org/streaming-sql-spec)]
- Schema-Aware PCollections [[doc](https://docs.google.com/document/d/1tnG2DPHZYbsomvihIpXruUmQ12pHGK0QIvXS1FOTgRc)]
- Pubsub to Beam SQL [[doc](https://docs.google.com/document/d/1554kJD33ovkBDvSNjasHu90L_EZOS26ZHr4ao1muS-A)]
- Apache Beam Proposal: design of DSL SQL interface [[doc](https://docs.google.com/document/d/1uWXL_yF3UUO5GfCxbL6kWsmC8xCWfICU3RwiQKsk7Mk)]
- Calcite/Beam SQL Windowing [[doc](https://docs.google.com/document/d/1yuG_fAnbAKEq3qz2jdf8qxyEIZ3xJAbCF1bbd_Y9Ia8)]
- Reject Unsupported Windowing Strategies in JOIN [[doc](https://docs.google.com/document/d/1Me0orPfH6vEFjfsTGcZ5ELWg-sw4st1ZvXqYyr7Pexc)]
- Beam DSL_SQL branch API review [[doc](https://s.apache.org/beam-sql-dsl-api-review)]
- Complex Types Support for Beam SQL DDL [[mail](https://lists.apache.org/thread.html/c494e521cb6865b1ae19a68e8e653afc562df7744e8d08087249cbe0@%3Cdev.beam.apache.org%3E)]
- [SQL] Reject unsupported inputs to Joins [[mail](https://lists.apache.org/thread.html/e7a442fa9cf6b76a5b435493170508f6c42fb9ccef9bcef434424f79@%3Cdev.beam.apache.org%3E)]
- Integrating runners & IO [[doc](https://docs.google.com/document/d/1ZFVlnldrIYhUgOfxIT2JcmTFFSWTl4HwAnQsnwiNL1g)]
- Beam SQL Pipeline Options [[doc](https://docs.google.com/document/d/1UTsSBuruJRfGnVOS9eXbQI6NauCD4WnSAPgA_Y0zjdk)]
- Unbounded limit [[doc](https://docs.google.com/document/d/13zeTewHH9nfwhSlcE4x77WQwr1U2Z4sTiNRjOXUj2aw)]

### Portability
- Fn API
  - Apache Beam Fn API Overview [[doc](https://s.apache.org/beam-fn-api)]
  - Processing a Bundle [[doc](https://s.apache.org/beam-fn-api-processing-a-bundle)]
  - Progress [[doc](https://s.apache.org/beam-fn-api-progress-reporting)]
  - Graphical view of progress [[doc](https://docs.google.com/document/d/1Dx18qBTvFWNqwLeecemOpKfleKzFyeV3Qwh71SHATvY)]
  - Fn State API and Bundle Processing [[doc](https://s.apache.org/beam-fn-state-api-and-bundle-processing)]
  - Checkpointing and splitting of Beam bundles over the Fn API, with application to SDF [[doc](https://s.apache.org/beam-breaking-fusion)]
  - How to send and receive data [[doc](https://s.apache.org/beam-fn-api-send-and-receive-data)]
  - Defining and adding SDK Metrics [[doc](https://s.apache.org/beam-fn-api-metrics)]
  - SDK harness container contract [[doc](https://s.apache.org/beam-fn-api-container-contract)]
  - Structure and Lifting of Combines [[doc](https://s.apache.org/beam-runner-api-combine-model)]
- Cross-language Beam Pipelines [[doc](https://s.apache.org/beam-mixed-language-pipelines)]
- SDK X with Runner Y using Runner API [[doc](https://s.apache.org/beam-job-api)]
- Flink Portable Runner Overview [[doc](https://s.apache.org/portable-flink-runner-overview)]
- Launching portable pipeline on Flink Runner [[doc](https://docs.google.com/document/d/1xOaEEJrMmiSHprd-WiYABegfT129qqF-idUBINjxz8s)]
- Portability support [[table](https://docs.google.com/spreadsheets/d/1KDa_FGn1ShjomGd-UUDOhuh2q73de2tPz6BqHpzqvNI)]
- Portability Prototype [[doc](https://s.apache.org/beam-portability-team-doc)]
- Portable Artifact Staging [[doc](https://docs.google.com/document/d/12zNk3O2nhTB8Zmxw5U78qXrvlk5r42X8tqF248IDlpI)]
- Portable Beam on Flink [[doc](https://s.apache.org/portable-beam-on-flink)]
- Portability API: How to Checkpoint and Split Bundles [[doc](https://s.apache.org/beam-checkpoint-and-split-bundles)]
- Portability API: How to Finalize Bundles [[doc](https://s.apache.org/beam-finalizing-bundles)]
- Side Input in Universal Reference Runner [[doc](https://docs.google.com/document/d/13N0OJ7QJm81wcgu13pi9GuN29UUxN2iIFn_H8lKpDks)]
- Spark Portable Runner Overview [[doc](https://docs.google.com/document/d/1j8GERTiHUuc6CzzCXZHc38rBn41uWfATBh2-5JN8hro)]
- Cross-Language Pipelines & Legacy IO [[doc](https://s.apache.org/beam-cross-language-io)]

### Build / Testing
- More Expressive PAsserts [[doc](https://docs.google.com/document/d/1fZUUbG2LxBtqCVabQshldXIhkMcXepsbv2vuuny8Ix4)]
- Mergebot design document [[doc](https://docs.google.com/document/d/18iFnW6egjqd_ADXCTQcuAkkz3J96LHdV5DlYUhXHf0M)]
- Performance tests for commonly used file-based I/O PTransforms [[doc](https://docs.google.com/document/d/1dA-5s6OHiP_cz-NRAbwapoKF5MEC1wKps4A5tFbIPKE)]
- Performance tests results analysis and basic regression detection [[doc](https://docs.google.com/document/d/1Cb7XVmqe__nA_WCrriAifL-3WCzbZzV4Am5W_SkQLeA)]
- Eventual PAssert [[doc](https://docs.google.com/document/d/1X_3KH_6QyfOSnh5kNK-fHlkEDrwPVpA2RnRggMMxhUk)]
- Testing I/O Transforms in Apache Beam [[doc](https://docs.google.com/document/d/153J9jPQhMCNi_eBzJfhAg-NprQ7vbf1jNVRgdqeEE8I)]
- Reproducible Environment for Jenkins Tests By Using Container [[doc](https://docs.google.com/document/d/1U7FeVMiHiBP-pFm4ULotqG1QqZY0fi7g9ZwTmeIgvvM)]
- Keeping precommit times fast [[doc](https://docs.google.com/document/d/1udtvggmS2LTMmdwjEtZCcUQy6aQAiYTI3OrTP8CLfJM/edit?usp=sharing)]
- Increase Beam post-commit tests stability [[doc](https://docs.google.com/document/d/1sczGwnCvdHiboVajGVdnZL0rfnr7ViXXAebBAf_uQME)]
- Beam-Site Automation Reliability [[doc](https://s.apache.org/beam-site-automation)]
- Managing outdated dependencies [[doc](https://docs.google.com/document/d/15m1MziZ5TNd9rh_XN0YYBJfYkt0Oj-Ou9g0KFDPL2aA)]
- Automation For Beam Dependency Check [[doc](https://docs.google.com/document/d/1rqr_8a9NYZCgeiXpTIwWLCL7X8amPAVfRXsO72BpBwA)]
- Test performance of core Apache Beam operations [[doc](https://s.apache.org/load-test-basic-operations)]
- Add static code analysis quality gates to Beam [[doc](https://docs.google.com/document/d/1YbV18mrHujmiLBtadS1WzCVeiI3Lo7W6awWJDA4A98o)]

### Python
- Beam Python User State and Timer APIs [[doc](https://s.apache.org/beam-python-user-state-and-timers)]
- Python Kafka connector [[doc](https://docs.google.com/document/d/1ogRS-e-HYYTHsXi_l2zDUUOnvfzEbub3BFkPrYIOawU)]
- Python 3 support [[doc](https://s.apache.org/beam-python-3)]
- Splittable DoFn for Python SDK [[doc](http://s.apache.org/splittable-do-fn-python-sdk)]
- Parquet IO for Python SDK [[doc](https://docs.google.com/document/d/1-FT6zmjYhYFWXL8aDM5mNeiUnZdKnnB021zTo4S-0Wg)]
- Building Python Wheels [[doc](https://docs.google.com/document/d/1MRVFs48e6g7wORshr2UpuOVD_yTSJTbmR65_j8XbGek)]

### Go
- Apache Beam Go SDK design [[doc](https://s.apache.org/beam-go-sdk-design-rfc)]
- Go SDK Vanity Import Path [[doc](https://s.apache.org/go-beam-vanity-import)]
- Go SDK Integration Tests [[doc](https://docs.google.com/document/d/1jy6EE7D4RjgfNV0FhD3rMsT1YKhnUfcHRZMAlC6ygXw)]

## Other
- Euphoria - High-Level Java 8 DSL [[doc](https://s.apache.org/beam-euphoria)]
- Apache Beam Code Review Guide [[doc](https://docs.google.com/document/d/1ZgAsSqEX9CaiTycrcR-tdc3X7MWlyT-F32jfMl89kDQ)]

Some of documents are available on this [google drive](https://drive.google.com/corp/drive/folders/0B-IhJZh9Ab52OFBVZHpsNjc4eXc)

To add new design document it is recommended to use this [design document template](https://docs.google.com/document/d/1kVePqjt2daZd0bQHGUwghlcLbhvrny7VpflAzk9sjUg)
