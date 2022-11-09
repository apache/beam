---
title:  "New Resources Available for Beam ML"
date:   2022-11-09 00:00:01 -0800
categories:
  - blog
  - python
authors:
  - damccorm
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

If you've been paying attention, over the past year you've noticed that
Beam has released a number of features designed to make Machine Learning
easy. Ranging from things like the introduction of the `RunInference`
transform to the continued refining of `Beam Dataframes`, this has been
an area where we've seen Beam make huge strides. While development has
advanced quickly, however, until recently there has been a lack of
resources to help people discover and use these new features.

Over the past several months, we've been hard at work building out
documentation and notebooks to make it easier to use these new features
and to show how Beam can be used to solve common Machine Learning problems.
We're now happy to present this new and improved Beam ML experience!

<img class="center-block"
     src="/images/blog/ensemble-model-notebook.png.png"
     alt="Example ensemble notebook with RunInference">

To get started, we encourage you to visit Beam's new [AI/ML landing page](https://beam.apache.org/documentation/ml/overview/).
We've got plenty of content on things like [multi-model pipelines](https://beam.apache.org/documentation/ml/multi-model-pipelines/),
[performing inference with metrics](https://beam.apache.org/documentation/ml/runinference-metrics/),
[online training](https://beam.apache.org/documentation/ml/online-clustering/), and much more.

We've also introduced a number of example [Jupyter Notebooks](https://github.com/apache/beam/tree/master/examples/notebooks/beam-ml)
showing how to use built in beam transforms like `RunInference` and `Beam Dataframes`.

We hope you find this useful! As always, if you see any areas for improvement, please [open an issue](https://github.com/apache/beam/issues/new/choose)
or a [pull request](https://github.com/apache/beam/pulls)!
