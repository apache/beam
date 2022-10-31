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
### Configuring pipeline options

Use the pipeline options to configure different aspects of your pipeline, such as the pipeline runner that will execute your pipeline and any runner-specific configuration required by the chosen runner. Your pipeline options will potentially include information such as your project ID or a location for storing files.

### Setting PipelineOptions from command-line arguments

While you can configure your pipeline by creating a `PipelineOptions` object and setting the fields directly, the Beam SDKs include a command-line parser that you can also use to set fields in `PipelineOptions` using command-line arguments.

To read options from the command-line, construct your `PipelineOptions` object as demonstrated in the following example code:

```
from apache_beam.options.pipeline_options import PipelineOptions

beam_options = PipelineOptions()
```

This interprets command-line arguments that follow this format:

```
--<option>=<value>
```

### Creating custom options

You can add your own custom options in addition to the standard `PipelineOptions`.

The following example shows how to add `input` and `output` custom options:

```
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input')
    parser.add_argument('--output')
```

You set the description and default value using annotations, as follows:

```
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='The file path for the input text to process.')
    parser.add_argument(
        '--output', required=True, help='The path prefix for output files.')
```

For Python, you can also simply parse your custom options with argparse; there is no need to create a separate PipelineOptions subclass.

### Playground exercise

You can find the full code of the above example in the playground window, which you can run and experiment with. And you can transfer files of other extensions. For example, a csv file with taxi order data. And after making some transformations, you can write to a new csv file:

```
--input=gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv --output=output.csv
```

Here is a small list of fields and an example record from this dataset:

| cost | passenger_count | ... |
|------|-----------------|-----|
| 5.8  | 1               | ... |
| 4.6  | 2               | ... |
| 24   | 1               | ... |

Overview [file](https://storage.googleapis.com/apache-beam-samples/nyc_taxi/misc/sample1000.csv)

Do you also notice in what order elements of PCollection appear in the console? Why is that? You can also run the example several times to see if the output stays the same or changes.