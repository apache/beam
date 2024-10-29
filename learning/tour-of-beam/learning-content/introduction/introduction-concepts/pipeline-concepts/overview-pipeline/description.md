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

### Overview

To use Beam, you first need to first create a driver program using the classes in one of the Beam SDKs. Your driver program defines your pipeline, including all of the inputs, transforms, and outputs. It also sets execution options for your pipeline (typically passed by using command-line options). These include the Pipeline Runner, which, in turn, determines what back-end your pipeline will run on.

The Beam SDKs provide several abstractions that simplify the mechanics of large-scale distributed data processing. The same Beam abstractions work with both batch and streaming data sources. When you create your Beam pipeline, you can think about your data processing task in terms of these abstractions. They include:

→ `Pipeline`: A Pipeline encapsulates your entire data processing task, from start to finish. This includes reading input data, transforming that data, and writing output data. All Beam driver programs must create a Pipeline. When you create the Pipeline, you must also specify the execution options that tell the Pipeline where and how to run.

→ `PCollection`: A PCollection represents a distributed data set that your Beam pipeline operates on. The data set can be bounded, meaning it comes from a fixed source like a file, or unbounded, meaning it comes from a continuously updating source via a subscription or other mechanism. Your pipeline typically creates an initial PCollection by reading data from an external data source, but you can also create a PCollection from in-memory data within your driver program. From there, PCollections are the inputs and outputs for each step in your pipeline.

→ `PTransform`: A PTransform represents a data processing operation, or a step, in your pipeline. Every PTransform takes zero or more PCollection objects as the input, performs a processing function that you provide on the elements of that PCollection, and then produces zero or more output PCollection objects.
{{if (eq .Sdk "go")}}

→ `Scope`: The Go SDK has an explicit scope variable used to build a `Pipeline`. A Pipeline can return it’s root scope with the `Root()` method. The scope variable is then passed to `PTransform` functions that place them in the `Pipeline` that owns the `Scope`.
{{end}}

→ `I/O transforms`: Beam comes with a number of “IOs” - library PTransforms that read or write data to various external storage systems.

A typical Beam driver program works as follows:

→ Create a Pipeline object and set the pipeline execution options, including the Pipeline Runner.

→ Create an initial `PCollection` for pipeline data, either using the IOs to read data from an external storage system, or using a Create transform to build a `PCollection` from in-memory data.

→ Apply `PTransforms` to each `PCollection`. Transforms can change, filter, group, analyze, or otherwise process the elements in a PCollection. A transform creates a new output PCollection without modifying the input collection. A typical pipeline applies subsequent transforms to each new output PCollection in turn until the processing is complete. However, note that a pipeline does not have to be a single straight line of transforms applied one after another: think of PCollections as variables and PTransforms as functions applied to these variables: the shape of the pipeline can be an arbitrarily complex processing graph.

→ Use IOs to write the final, transformed PCollection(s) to an external source.

→ Run the pipeline using the designated Pipeline Runner.

When you run your Beam driver program, the Pipeline Runner that you designate constructs a workflow graph of your pipeline based on the PCollection objects you’ve created and the transforms that you’ve applied. That graph is then executed using the appropriate distributed processing back-end, becoming an asynchronous “job” (or equivalent) on that back-end.