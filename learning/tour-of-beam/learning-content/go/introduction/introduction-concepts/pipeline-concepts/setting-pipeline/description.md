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

Use Go flags to parse command line arguments to configure your pipeline. Flags must be parsed before `beam.Init()` is called.

```
// If beamx or Go flags are used, flags must be parsed first,
// before beam.Init() is called.
flag.Parse()
```

This interprets command-line arguments this follow the format:

```
--<option>=<value>
```

### Creating custom options

You can add your own custom options in addition to the standard `PipelineOptions`.

The following example shows how to add `input` and `output` custom options:

```
// Use standard Go flags to define pipeline options.
var (
  input  = flag.String("input", "gs://my-bucket/input", "Input for the pipeline")
  output = flag.String("output", "gs://my-bucket/output", "Output for the pipeline")
)
```

### Playground exercise

You can find the full code of the above example in the playground window, which you can run and experiment with.

You can transfer files of other extensions. For example, a csv file with taxi order data. And after making some transformations, you can write to a new csv file:
```
var (
  input = flag.String("input", "gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv", "File(s) to read.")

  output = flag.String("output", "output.csv", "Output file (required).")
)
```

Here is a small list of fields and an example record from this dataset:

| cost | passenger_count | ... |
|------|-----------------|-----|
| 5.8  | 1               | ... |
| 4.6  | 2               | ... |
| 24   | 1               | ... |

Overview [file](https://storage.googleapis.com/apache-beam-samples/nyc_taxi/misc/sample1000.csv)

Do you also notice in what order elements of PCollection appear in the console? Why is that? You can also run the example several times to see if the output stays the same or changes.