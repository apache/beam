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

The `Pipeline` abstraction encapsulates all the data and steps in your data processing task. Your Beam driver program typically starts by constructing a `Pipeline` object, and then using that object as the basis for creating the pipeline’s data sets as `PCollection`s and its operations as `Transforms`.

To use Beam, your driver program must first create an instance of the Beam SDK class `Pipeline` (typically in the main() function). When you create your `Pipeline`, you’ll also need to set some configuration options. You can set your pipeline’s configuration options programmatically, but it’s often easier to set the options ahead of time (or read them from the command line) and pass them to the `Pipeline` object when you create the object.

```
// Start by defining the options for the pipeline.
PipelineOptions options = PipelineOptionsFactory.create();

// Then create the pipeline.
Pipeline p = Pipeline.create(options);
```

### Playground exercise

You can find the full code of the above example in the playground window, which you can run and experiment with.

When creating pipelines when writing arguments (String args[]), you can explicitly specify runner:

```
--runner=DirectRunner
```