<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Writing a Dataflow Cost Benchmark

Writing a Dataflow Cost Benchmark to estimate the financial cost of executing a pipeline on Google Cloud Platform Dataflow requires 4 components in the repository:

1. A pipeline to execute (ideally one located in the examples directory)
1. A text file with pipeline options in the `.github/workflows/cost-benchmarks-pipeline-options` directory
1. A test class inheriting from the `DataflowCostBenchmark` class 
1. An entry to execute the pipeline as part of the cost benchmarks workflow action 

### Choosing a Pipeline
Pipelines that are worth benchmarking in terms of performance and cost have a few straightforward requirements. 

1. The transforms used in the pipeline should be native to Beam *or* be lightweight and readily available in the given pipeline
1. The pipeline itself should run on a consistent data set and have consistent internals (such as model versions for `RunInference` workloads.)
1. The pipeline should perform some sort of behavior that would be common enough for a user to create themselves
    * Effectively, we want to read data from a source, do some sort of transformation, then write that data elsewhere. No need to overcomplicate things. 

Additionally, the `run()` call for the pipeline should return a `PipelineResult` object, which the benchmark framework uses to query metrics from Dataflow after the run completes. 

### Pipeline Options
Once you have a functioning pipeline to configure as a benchmark, the options needs to be saved as a `.txt` file in the `.github/workflows/cost-benchmarks-pipeline-options` directory. The file needs the Apache 2.0 license header at the top of the file, then each flag will need to be provided on a separate line. These arguments include:

* GCP Region (usually us-central1)
* Machine Type
* Number of Workers
* Disk Size
* Autoscaling Algorithm (typically `NONE` for benchmarking purposes)
* Staging and Temp locations
* A requirements file path in the repository (if additional dependencies are needed)
* Benchmark-specific values
    * `publish_to_big_query` - always true for benchmarks
    * Metrics Dataset for Output
    * Metrics Table for Output

### Configuring the Test Class
With the pipeline itself chosen and the arguments set, we can build out the test class that will execute the pipeline. Navigate to `sdks/python/apache_beam/testing/benchmarks` and select an appropriate sub-directory (or create one if necessary.) The class for `wordcount` is shown below:

```py
import logging

from apache_beam.examples import wordcount
from apache_beam.testing.load_tests.dataflow_cost_benchmark import DataflowCostBenchmark


class WordcountCostBenchmark(DataflowCostBenchmark):
  def __init__(self):
    super().__init__()

  def test(self):
    extra_opts = {}
    extra_opts['output'] = self.pipeline.get_option('output_file')
    self.result = wordcount.run(
        self.pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  WordcountCostBenchmark().run()
```

The important notes here: if there are any arguments with common arg names (like `input` and `output`) you can use the `extra_opts` dictionary to map to them from alternatives in the options file. 

You should also make sure that you save the output of the `run()` call from the pipeline in the `self.result` field, as the framework will try to re-run the pipeline without the extra opts if that value is missing. Beyond those two key notes, the benchmarking framework does all of the other work in terms of setup, teardown, and metrics querying.

### Updating the Benchmark Workflow
Navigate to `.github/workflows/beam_Python_CostBenchmarks_Dataflow.yml` and make the following changes:

1. Add the pipeline options `.txt` file written above to the `argument-file-paths` list. This will load those pipeline options as an entry in the workflow environment, with the entry getting the value `env.beam_Python_Cost_Benchmarks_Dataflow_test_arguments_X`. X is an integer value corresponding to the position of the text file in the list of files, starting from `1`.
2. Create an entry for the benchmark. The entry for wordcount is as follows:

```yaml
      - name: Run wordcount on Dataflow
        uses: ./.github/actions/gradle-command-self-hosted-action
        timeout-minutes: 30
        with:
	@@ -88,4 +92,14 @@ jobs:
            -PloadTest.mainClass=apache_beam.testing.benchmarks.wordcount.wordcount \
            -Prunner=DataflowRunner \
            -PpythonVersion=3.10 \
            '-PloadTest.args=${{ env.beam_Inference_Python_Benchmarks_Dataflow_test_arguments_1 }} --job_name=benchmark-tests-wordcount-python-${{env.NOW_UTC}} --output_file=gs://temp-storage-for-end-to-end-tests/wordcount/result_wordcount-${{env.NOW_UTC}}.txt' \
```

The main class is the `DataflowCostBenchmark` subclass defined earlier, then the runner and python version are specified. The majority of pipeline arguments are loaded from the `.txt` file, with the job name and output being specified here. Be sure to set a reasonable timeout here as well. 