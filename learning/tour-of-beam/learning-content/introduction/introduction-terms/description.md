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
**Pipeline** - A pipeline is a user-constructed graph of transformations that defines the desired data processing operations.

**PCollection** - A PCollection is a data set or data stream. The data that a pipeline processes is part of a PCollection.

**PTransform** - A PTransform (or transform) represents a data processing operation, or a step, in your pipeline. A transform is applied to zero or more PCollection objects, and produces zero or more PCollection objects.

**Aggregation** - Aggregation is computing a value from multiple (1 or more) input elements.

**User-defined function (UDF)** - Some Beam operations allow you to run user-defined code as a way to configure the transform.

**Schema** - A schema is a language-independent type definition for a PCollection. The schema for a PCollection defines elements of that PCollection as an ordered list of named fields.

**SDK** - A language-specific library that lets pipeline authors build transforms, construct their pipelines, and submit them to a runner.

**Runner** - A runner runs a Beam pipeline using the capabilities of your chosen data processing engine.

**Window** - A PCollection can be subdivided into windows based on the timestamps of the individual elements. Windows enable grouping operations over collections that grow over time by dividing the collection into windows of finite collections.

**Watermark** - A watermark is a guess as to when all data in a certain window is expected to have arrived. This is needed because data isn’t always guaranteed to arrive in a pipeline in event time order, or to always arrive at predictable intervals.

**Trigger** - A trigger determines when to aggregate the results of each window.

**State and timers** - Per-key state and timer callbacks are lower level primitives that give you full control over aggregating input collections that grow over time.

**Splittable DoFn** - Splittable DoFns let you process elements in a non-monolithic way. You can checkpoint the processing of an element, and the runner can split the remaining work to yield additional parallelism.