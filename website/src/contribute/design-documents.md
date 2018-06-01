---
layout: section
title: 'Beam Design Documents'
section_menu: section-menu/contribute.html
permalink: /contribute/design-documents/
---

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
- Splittable DoFn (Obsoletes Source API) [[doc](http://s.apache.org/splittable-do-fn)]
  - Reimplementing Beam API classes on top of Splittable DoFn on top of Source API [[doc](https://s.apache.org/sdf-via-source)]
  - New TextIO features based on SDF [[doc](http://s.apache.org/textio-sdf)]
  - Watch transform [[doc](http://s.apache.org/beam-watch-transform)]
- State and Timers for DoFn [[doc](https://s.apache.org/beam-state)]
- ContextFn [[doc](http://s.apache.org/context-fn)]
- Static Display Data [[doc](https://docs.google.com/document/d/11enEB9JwVp6vO0uOYYTMYTGkr3TdNfELwWqoiUg5ZxM)]
- Lateness (and Panes) in Apache Beam [[doc](https://s.apache.org/beam-lateness)]
- Triggers in Apache Beam [[doc](https://s.apache.org/beam-triggers)]
- Triggering is for sinks [[doc](https://s.apache.org/beam-sink-triggers)] (not implemented)
- Pipeline Drain [[doc](https://docs.google.com/document/d/1NExwHlj-2q2WUGhSO4jTu8XGhDPmm3cllSN8IMmWci8)]
- Pipelines Considered Harmful [[doc](https://s.apache.org/no-beam-pipeline)]

### IO / Filesystem
- IOChannelFactory Redesign [[doc](https://docs.google.com/document/d/11TdPyZ9_zmjokhNWM3Id-XJsVG3qel2lhdKTknmZ_7M)]
- Configurable BeamFileSystem [[doc](https://docs.google.com/document/d/1-7vo9nLRsEEzDGnb562PuL4q9mUiq_ZVpCAiyyJw8p8)]
- New API for writing files in Beam [[doc](http://s.apache.org/fileio-write)]

### Metrics
- Metrics API [[doc](http://s.apache.org/beam-metrics-api)]
- I/O Metrics [[doc](https://s.apache.org/standard-io-metrics)]
- Metrics extraction independent from runners / execution engines [[doc](https://s.apache.org/runner_independent_metrics_extraction)]

### Runners
- Runner Authoring Guide [[doc](https://s.apache.org/beam-runner-guide)] (obsoletes [[doc](http://s.apache.org/beam-runner-api)] and [[doc](https://s.apache.org/beam-runner-1-pager)])
- Composite PInputs, POutputs, and the Runner API [[doc](https://s.apache.org/beam-runner-composites)]
- Side Input Architecture for Apache Beam [[doc](https://s.apache.org/beam-side-inputs-1-pager)]

### SQL / Schema
- Streams and Tables [[doc](https://s.apache.org/beam-streams-tables)]
- Streaming SQL [[doc](http://s.apache.org/streaming-sql-spec)]
- Schema-Aware PCollections [[doc](https://docs.google.com/document/d/1tnG2DPHZYbsomvihIpXruUmQ12pHGK0QIvXS1FOTgRc)]

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

### Testing
- More Expressive PAsserts [[doc](https://docs.google.com/document/d/1fZUUbG2LxBtqCVabQshldXIhkMcXepsbv2vuuny8Ix4)]
- Mergebot design document [[doc](https://docs.google.com/document/d/18iFnW6egjqd_ADXCTQcuAkkz3J96LHdV5DlYUhXHf0M)]
- Performance tests for commonly used file-based I/O PTransforms [[doc](https://docs.google.com/document/d/1dA-5s6OHiP_cz-NRAbwapoKF5MEC1wKps4A5tFbIPKE)]
- Performance tests results analysis and basic regression detection [[doc](https://docs.google.com/document/d/1Cb7XVmqe__nA_WCrriAifL-3WCzbZzV4Am5W_SkQLeA)]

### Python
- Beam Python User State and Timer APIs [[doc](https://s.apache.org/beam-python-user-state-and-timers)]
- Python Kafka connector [[doc](https://docs.google.com/document/d/1ogRS-e-HYYTHsXi_l2zDUUOnvfzEbub3BFkPrYIOawU)]

### Go
- Apache Beam Go SDK design [[doc](https://s.apache.org/beam-go-sdk-design-rfc)]
- Go SDK Vanity Import Path [[doc](https://s.apache.org/go-beam-vanity-import)]

## Other
Some of documents are available on this [google drive](https://drive.google.com/corp/drive/folders/0B-IhJZh9Ab52OFBVZHpsNjc4eXc)

To add new design document it is recommended to use this [design document template](https://docs.google.com/document/d/1kVePqjt2daZd0bQHGUwghlcLbhvrny7VpflAzk9sjUg)