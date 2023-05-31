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
### Creating a pipeline

The `Pipeline` abstraction encapsulates all the data and steps in your data processing task. Your Beam driver program typically starts by constructing a Pipeline object, and then using that object as the basis for creating the pipeline’s data sets as PCollections and its operations as `Transforms`.

To use Beam, your driver program must first create an instance of the Beam SDK class Pipeline (typically in the main() function). When you create your `Pipeline`, you’ll also need to set some configuration options. You can set your pipeline’s configuration options programmatically, but it’s often easier to set the options ahead of time (or read them from the command line) and pass them to the Pipeline object when you create the object.
{{if (eq .Sdk "go")}}
```
// beam.Init() is an initialization hook that must be called
// near the beginning of main(), before creating a pipeline.
beam.Init()

// Create the Pipeline object and root scope.
pipeline, scope := beam.NewPipelineWithRoot()
```
{{end}}
{{if (eq .Sdk "java")}}
```
// Start by defining the options for the pipeline.
PipelineOptions options = PipelineOptionsFactory.create();

// Then create the pipeline.
Pipeline pipeline = Pipeline.create(options);
```
{{end}}
{{if (eq .Sdk "python")}}
```
import apache_beam as beam

with beam.Pipeline() as p:
  pass  # build your pipeline here
```
{{end}}
### Playground exercise

You can find the full code of the above example in the playground window, which you can run and experiment with.

{{if (eq .Sdk "go")}}
And you can create a `pipeline`, `scope` separately, it is an alternative to `beam.NewPipelineWithRoot()`. It is convenient if manipulations are needed before creating an element.

```
pipeline := beam.NewPipeline()
scope := p.Root()
```
{{end}}
{{if (eq .Sdk "java")}}
When creating pipelines when writing arguments (String args[]), you can explicitly specify runner:

```
--runner=DirectRunner
```
{{end}}
{{if (eq .Sdk "python")}}
When creating pipelines, you can give an argument with explicitly specified parameters:

```
beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='my-project-id',
    job_name='unique-job-name',
    temp_location='gs://my-bucket/temp',
)
```

Creation based on option:

```
pipeline = beam.Pipeline(options=beam_options)
```
{{end}}