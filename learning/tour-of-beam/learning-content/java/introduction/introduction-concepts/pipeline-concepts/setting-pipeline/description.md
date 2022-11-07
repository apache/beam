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

When you run the pipeline on a runner of your choice, a copy of the PipelineOptions will be available to your code. For example, if you add a PipelineOptions parameter to a DoFn’s `@ProcessElement` method, it will be populated by the system.

### Setting PipelineOptions from command-line arguments

While you can configure your pipeline by creating a PipelineOptions object and setting the fields directly, the Beam SDKs include a command-line parser that you can use to set fields in PipelineOptions using command-line arguments.

To read options from the command-line, construct your PipelineOptions object as demonstrated in the following example code:

```
PipelineOptions options =
    PipelineOptionsFactory.fromArgs(args).withValidation().create();
```

This interprets command-line arguments that follow the format:

```
--<option>=<value>
```

> Appending the method .withValidation will check for required command-line arguments and validate argument values.

### Creating custom options

You can add your own custom options in addition to the standard `PipelineOptions`.

To add your own options, define an interface with getter and setter methods for each option.

The following example shows how to add `input` and `output` custom options:

```
public interface MyOptions extends PipelineOptions {
    String getInput();
    void setInput(String input);

    String getOutput();
    void setOutput(String output);
}
```

You set the description and default value using annotations, as follows:

```
public interface MyOptions extends PipelineOptions {
    @Description("Input for the pipeline")
    @Default.String("gs://my-bucket/input")
    String getInput();
    void setInput(String input);

    @Description("Output for the pipeline")
    @Default.String("gs://my-bucket/output")
    String getOutput();
    void setOutput(String output);
}
```

It’s recommended that you register your interface with `PipelineOptionsFactory` and then pass the interface when creating the `PipelineOptions` object. When you register your interface with `PipelineOptionsFactory`, the `--help` can find your custom options interface and add it to the output of the --help command. `PipelineOptionsFactory` will also validate that your custom options are compatible with all other registered options.

The following example code shows how to register your custom options interface with `PipelineOptionsFactory`:

```
PipelineOptionsFactory.register(MyOptions.class);
MyOptions options = PipelineOptionsFactory.fromArgs(args)
                                                .withValidation()
                                                .as(MyOptions.class);
```

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