---
title:  "Python SDK released in Apache Beam 0.6.0"
date:   2017-03-16 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2017/03/16/python-sdk-release.html
authors:
  - altay
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

Apache Beam’s latest release, version [0.6.0](/get-started/downloads/), introduces a new SDK -- this time, for the Python programming language. The Python SDK joins the Java SDK as the second implementation of the Beam programming model.

<!--more-->

The Python SDK incorporates all of the main concepts of the Beam model, including ParDo, GroupByKey, Windowing, and others. It features extensible IO APIs for writing bounded sources and sinks, and provides built-in implementation for reading and writing Text, Avro, and TensorFlow record files, as well as connectors to Google BigQuery and Google Cloud Datastore.

There are two runners capable of executing pipelines written with the Python SDK today: [Direct Runner](/documentation/runners/direct/) and [Dataflow Runner](/documentation/runners/dataflow/), both of which are currently limited to batch execution only. Upcoming features will shortly bring the benefits of the Python SDK to additional runners.

#### Try the Apache Beam Python SDK

If you would like to try out the Python SDK, a good place to start is the [Quickstart](/get-started/quickstart-py/). After that, you can take a look at additional [examples](https://github.com/apache/beam/tree/v0.6.0/sdks/python/apache_beam/examples), and deep dive into the [API reference](https://beam.apache.org/releases/pydoc/).

Let’s take a look at a quick example together. First, install the `apache-beam` package from PyPI and start your Python interpreter.

```
$ pip install apache-beam
$ python
```

We will harness the power of Apache Beam to estimate Pi in honor of the recently passed Pi Day.

```
import random
import apache_beam as beam

def run_trials(count):
  """Throw darts into unit square and count how many fall into unit circle."""
  inside = 0
  for _ in xrange(count):
    x, y = random.uniform(0, 1), random.uniform(0, 1)
    inside += 1 if x*x + y*y <= 1.0 else 0
  return count, inside

def combine_results(results):
  """Given all the trial results, estimate pi."""
  total, inside = sum(r[0] for r in results), sum(r[1] for r in results)
  return total, inside, 4 * float(inside) / total if total > 0 else 0

p = beam.Pipeline()
(p | beam.Create([500] * 10)  # Create 10 experiments with 500 samples each.
   | beam.Map(run_trials)     # Run experiments in parallel.
   | beam.CombineGlobally(combine_results)      # Combine the results.
   | beam.io.WriteToText('./pi_estimate.txt'))  # Write PI estimate to a file.

p.run()
```

This example estimates Pi by throwing random darts into the unit square and keeping track of the fraction of those darts that fell into the unit circle (see the full [example](https://github.com/apache/beam/blob/v0.6.0/sdks/python/apache_beam/examples/complete/estimate_pi.py) for details). If you are curious, you can check the result of our estimation by looking at the output file.

```
$ cat pi_estimate.txt*
```

#### Roadmap

The first thing on the Python SDK’s roadmap is to address two of its limitations. First, the existing runners are currently limited to bounded PCollections, and we are looking forward to extending the SDK to support unbounded PCollections (“streaming”). Additionally, we are working on extending support to more Apache Beam runners, and the upcoming Fn API will do the heavy lifting.

Both of these improvements will enable the Python SDK to fulfill the mission of Apache Beam: a unified programming model for batch and streaming data processing that can run on any execution engine.

#### Join us!

Please consider joining us, whether as a user or a contributor, as we work towards our first release with API stability. If you’d like to try out Apache Beam today, check out the latest [0.6.0](/get-started/downloads/) release. We welcome contributions and participation from anyone through our mailing lists, issue tracker, pull requests, and events.
