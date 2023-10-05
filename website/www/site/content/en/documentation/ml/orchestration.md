---
title: "Orchestration"
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

# Workflow orchestration

This page provides KFP and TFX orchestration examples. It first provides the KFP example, and then it demonstrates how TFX manages functionality that is defined by hand when using KFP.

## Understanding the Beam DAG


Apache Beam is an open source, unified model for defining both batch and streaming data-parallel processing pipelines. A concept central to the Apache Beam programming model is the Directed Acyclic Graph (DAG). Each Apache Beam pipeline is a DAG that you can construct through the Beam SDK in your programming language of choice (from the set of supported Apache Beam SDKs). Each node of this DAG represents a processing step (`PTransform`) that accepts a collection of data as input (`PCollection`) and then outputs a transformed collection of data (`PCollection`). The edges define how data flows through the pipeline from one processing step to another. The following diagram shows an example pipeline workflow.

![A standalone beam pipeline](/images/standalone-beam-pipeline.svg)

Defining a pipeline and the corresponding DAG does not mean that data starts flowing through the pipeline. To run the pipeline, you need to deploy it to one of the [supported Beam runners](/documentation/runners/capability-matrix/). These distributed processing backends include Apache Flink, Apache Spark, and Google Cloud Dataflow. To run the pipeline locally on your machine for development and debugging purposes, a [Direct Runner](/documentation/runners/direct/) is also provided. View the [runner capability matrix](/documentation/runners/capability-matrix/) to verify that your chosen runner supports the data processing steps defined in your pipeline, especially when using the Direct Runner.

## Orchestrating frameworks

Successfully delivering machine learning projects requires more than training a model. A full ML workflow often contains a range of other steps, including data ingestion, data validation, data preprocessing, model evaluation, model deployment, data drift detection, and so on. Furthermore, you need to track metadata and artifacts from your experiments to answer important questions, such as:
- What data was this model trained on and with which training parameters?
- When was this model deployed and what accuracy did it have on a test dataset?
Without this knowledge, troubleshooting, monitoring, and improving your ML solutions becomes increaseingly difficult when your solutions grow in size.

The solution: MLOps. MLOps is an umbrella term used to describe best practices and guiding principles that aim to make the development and maintenance of machine learning systems seamless and efficient. MLOps most often entails automating machine learning workflows throughout the model and data lifecycle. Popular frameworks to create these workflow DAGs are [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/introduction/), [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html), and [TFX](https://www.tensorflow.org/tfx/guide).

You can either use an Apache Beam pipeline as a standalone data processing job, or you can make it part of a larger sequence of steps in a workflow. In the latter case, the Apache Beam DAG is one node in the overarching DAG composed by the workflow orchestrator. This workflow thus contains a DAG withinin a DAG, as illustrated in the following diagram.

![An Apache Beam pipeline as part of a larger orchestrated workflow](/images/orchestrated-beam-pipeline.svg)

The key difference between the Apache Beam DAG and the orchestrating DAG is that the Apache Beam DAG processes data and passes that data between the nodes of its DAG, whereas the orchestration DAG schedules and monitors steps in the workflow and passes execution parameters, metadata, and artifacts between the nodes of the DAG.
- Apache Beam focuses on parallelization and enabling both batch and streaming jobs.
- Examples of orchestration DAG artifacts include trained models and datasets. Such artifacts are often passed by a reference URI and not by value.

Note: TFX creates a workflow DAG, which needs an orchestrator of its own to run. [Natively supported orchestrators for TFX](https://www.tensorflow.org/tfx/guide/custom_orchestrator) are Airflow, Kubeflow Pipelines, and Apache Beam itself. As mentioned in the [TFX docs](https://www.tensorflow.org/tfx/guide/beam_orchestrator):

> "Several TFX components rely on Beam for distributed data processing. In addition, TFX can use Apache Beam to orchestrate and execute the pipeline DAG. Beam orchestrator uses a different BeamRunner than the one which is used for component data processing."

Caveat: The Beam orchestrator is not meant to be a TFX orchestrator used in production environments. It simply enables debugging TFX pipelines locally on Beam’s Direct Runner without the need for the extra setup required for Airflow or Kubeflow.

## Preprocessing example

This section describes two orchestrated ML workflows, one with Kubeflow Pipelines (KFP) and one with Tensorflow Extended (TFX). These two frameworks both create workflows but have their own distinct advantages and disadvantages:
- KFP requires you to create your workflow components from scratch, and requires a user to explicitly indicate which artifacts should be passed between components and in what way.
- TFX offers a number of prebuilt components and takes care of the artifact passing more implicitly.
 When choosing between the two frameworks, you need to consider the trade-off between flexibility and programming overhead.

For simplicity, the workflows only contain three components: data ingestion, data preprocessing, and model training. Depending on the scenario, you can add a range of extra components, such as model evaluation and model deployment. This example focuses on the preprocessing component, because it demonstrates how to use Apache Beam in an ML workflow for efficient and parallel processing of your ML data.

The dataset consists of images paired with a textual caption describing the content of the image. These pairs are taken from a captions subset of the [MSCOCO 2014 dataset](https://cocodataset.org/#home). This multi-modal data (image and text) gives us the opportunity to experiment with preprocessing operations for both modalities.

### Kubeflow pipelines (KFP)

In order to run our ML workflow with KFP we must perform three steps:

1. Create the KFP components by specifying the interface to the components and by writing and containerizing the implementation of the component logic.
2. Create the KFP pipeline by connecting the created components, specifying how inputs and outputs should be passed between components, and compiling the pipeline definition to a full pipeline definition.
3. Run the KFP pipeline by submitting it to a KFP client endpoint.

The full example code can be found in the [GitHub repository](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/kfp)

#### Create the KFP components

The following diagram shows our target file structure:

        kfp
        ├── pipeline.py
        ├── components
        │   ├── ingestion
        │   │   ├── Dockerfile
        │   │   ├── component.yaml
        │   │   ├── requirements.txt
        │   │   └── src
        │   │       └── ingest.py
        │   ├── preprocessing
        │   │   ├── Dockerfile
        │   │   ├── component.yaml
        │   │   ├── requirements.txt
        │   │   └── src
        │   │       └── preprocess.py
        │   └── train
        │       ├── Dockerfile
        │       ├── component.yaml
        │       ├── requirements.txt
        │       └── src
        │           └── train.py
        └── requirements.txt

The full preprocessing component specification is shown in the folllowing illustration. The inputs are the path where the ingested dataset was saved by the ingest component and a path to a directory where the component can store artifacts. Additionally, some inputs specify how and where the Apache Beam pipeline runs. The specifications for the ingestion and train components are similar and can be found in the [ingestion component.yaml](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/kfp/components/ingestion/component.yaml) file and in the [train component.yaml](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/kfp/components/train/component.yaml) file, respectively.

>Note: we are using the KFP v1 SDK, because v2 is still in [beta](https://www.kubeflow.org/docs/started/support/#application-status). The v2 SDK introduces some new options for specifying the component interface with more native support for input and output artifacts. To see how to migrate components from v1 to v2, consult the [KFP docs](https://www.kubeflow.org/docs/components/pipelines/sdk-v2/v2-component-io/).

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/component.yaml" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/component.yaml" preprocessing_component_definition >}}
{{< /highlight >}}

In this case, each component shares an identical Dockerfile, but you can add extra component-specific dependencies where needed.

{{< highlight language="Dockerfile" file="sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/Dockerfile" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/Dockerfile" component_dockerfile >}}
{{< /highlight >}}

With the component specification and containerization done, implement the preprocessing component.

Because KFP provides the input and output arguments as command-line arguments, an `argumentparser` is needed.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kf/components/preprocessing/src/preprocess.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" preprocess_component_argparse >}}
{{< /highlight >}}

The implementation of the `preprocess_dataset` function contains the Apache Beam pipeline code and the Beam pipeline options that select the runner. The executed preprocessing involves downloading the image bytes from their URL, converting them to a Torch Tensor, and resizing to the desired size. The caption undergoes a series of string manipulations to ensure that our model receives uniform image descriptions. Tokenization is not done here, but could be included here if the vocabulary is known. Finally, each element is serialized and written to [Avro](https://avro.apache.org/docs/) files. You can use alternative files formats, such as TFRecords.


{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" deploy_preprocessing_beam_pipeline >}}
{{< /highlight >}}

It also contains the necessary code to perform the component I/O. First, a target path is constructed to store the preprocessed dataset based on the component input parameter `base_artifact_path` and a timestamp. Output values from components are only returned as files, so we write the value of the constructed target path to an output file that was provided to our component by KFP.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" kfp_component_input_output >}}
{{< /highlight >}}

Because we are mainly interested in the preprocessing component to show how a Beam pipeline can be integrated into a larger ML workflow, this section doesn't cover the implementation of the ingestion and train components in depth. Implementations of dummy components that mock their behavior are provided in the full example code.

#### Create the pipeline definition

`pipeline.py` first loads the created components from their specification `.yaml` file.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" load_kfp_components >}}
{{< /highlight >}}

After that, the pipeline is created, and the required component inputs and outputs are specified manually.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" define_kfp_pipeline >}}
{{< /highlight >}}

Finally, the defined pipeline is compiled, and a `pipeline.json` specification file is generated.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" compile_kfp_pipeline >}}
{{< /highlight >}}


#### Run the KFP pipeline

Using the following specification file and snippet with the necessary [requirements](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/kfp/requirements.txt) installed, you can now run the pipeline. Consult the [`run_pipeline` documentation](https://kubeflow-pipelines.readthedocs.io/en/latest/source/kfp.client.html#kfp.Client.run_pipeline) for more information. Before running the pipeline, a container for each component must be built and pushed to a container registry that your pipeline can access. Also, the component specification `.yaml` files must point to the correct container image.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" execute_kfp_pipeline >}}
{{< /highlight >}}


### Tensorflow Extended (TFX)

Working with TFX is similar to the approach for KFP illustrated previously: Define the individual workflow components, connect them in a pipeline object, and run the pipeline in the target environment. What makes TFX different is that it has already built a set of Python packages that are libraries to create workflow components. Unlike with the KFP example, you don't need to start from scratch by writing and containerizing the code.

With TFX, you need to choose which TFX components are relevant to your workflow and use the library to adapt their functionality to your use case. The following diagram shows the available components and their corresponding libraries.

![TFX libraries and components](https://www.tensorflow.org/static/tfx/guide/images/libraries_components.png)

TFX relies heavily on Apache Beam to implement data-parallel pipelines in these libraries. You need to run components created with these libraries with one of the supported Apache Beam runners. The full TFX example code can again be found in the [GitHub repository](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/tfx).

For the KFP example, we used ingestion, preprocessing, and trainer components. In this TFX example, we use the ExampleGen, Transform, and Trainer libraries.

First, review the pipeline definition. Note that this definition looks similar to our previous example.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_local.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_local.py" tfx_pipeline >}}
{{< /highlight >}}

We use the same data input, that is, a couple of image-captions pairs extracted from the [MSCOCO 2014 dataset](https://cocodataset.org/#home). This time, however, we use the data in CSV format, because the ExampleGen component does not by default support jsonlines. The formats that are supported out of the box are listed in the [Data Sources and Formats](https://www.tensorflow.org/tfx/guide/examplegen#data_sources_and_formats) page in the TensorFlow documentation. Alternatively, you can write a [custom ExampleGen](https://www.tensorflow.org/tfx/guide/examplegen#custom_examplegen).

Copy the snippet below to an input data CSV file:

{{< highlight >}}
image_id,id,caption,image_url,image_name,image_license
318556,255,"An angled view of a beautifully decorated bathroom.","http://farm4.staticflickr.com/3133/3378902101_3c9fa16b84_z.jpg","COCO_train2014_000000318556.jpg","Attribution-NonCommercial-ShareAlike License"
476220,14,"An empty kitchen with white and black appliances.","http://farm7.staticflickr.com/6173/6207941582_b69380c020_z.jpg","COCO_train2014_000000476220.jpg","Attribution-NonCommercial License"
{{< /highlight >}}

So far, we have only imported standard TFX components and chained them together into a pipeline. Both the Transform and the Trainer components have a `module_file` argument defined. That’s where we define the behavior we want from these standard components.

#### Preprocess

The Transform component searches the `module_file` for a definition of the function `preprocessing_fn`. This function is the central concept of the `tf.transform` library. The [TFX documentation](https://www.tensorflow.org/tfx/transform/get_started#define_a_preprocessing_function) describes this function:

> The preprocessing function is the most important concept of tf.Transform. The preprocessing function is a logical description of a transformation of the dataset. The preprocessing function accepts and returns a dictionary of tensors, where a tensor means Tensor or SparseTensor. There are two kinds of functions used to define the preprocessing function:
>1. Any function that accepts and returns tensors. These add TensorFlow operations to the graph that transform raw data into transformed data.
>2. Any of the analyzers provided by tf.Transform. Analyzers also accept and return tensors, but unlike TensorFlow functions, they do not add operations to the graph. Instead, analyzers cause tf.Transform to compute a full-pass operation outside of TensorFlow. They use the input tensor values over the entire dataset to generate a constant tensor that is returned as the output. For example, tft.min computes the minimum of a tensor over the dataset. tf.Transform provides a fixed set of analyzers, but this will be extended in future versions.

Therefore, `preprocesing_fn` can contain all tf operations that accept and return tensors, as well as specific `tf.transform` operations. In the following example, we use the former to convert all incoming captions to lowercase letters only, while the latter does a full pass on all the data in our dataset to compute the average length of the captions used for a follow-up preprocessing step.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" tfx_preprocessing_fn >}}
{{< /highlight >}}

This function only defines the logical steps that must be performed during preprocessing. The function needs a concrete implementation before it can run. One such implementation is provided by `tf.Transform` using Apache Beam, which provides a PTransform `tft_beam.AnalyzeAndTransformDataset` to process the data. We can test this preproccesing_fn outside of the TFX Transform component using this PTransform explicitly. Calling the `processing_fn` in such a way is not necessary when using `tf.Transform` in combination with the TFX Transform component.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" tfx_analyze_and_transform >}}
{{< /highlight >}}

#### Train

The Trainer component behaves like the Transform component, but instead of looking for a `preprocessing_fn`, it requires a `run_fn` function in the specified `module_file`. Our simple implementation creates a stub model using `tf.Keras` and saves the resulting model to a directory.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" tfx_run_fn >}}
{{< /highlight >}}

#### Executing the pipeline

To launch the pipeline, provide two configurations: The orchestrator for the TFX pipeline and the pipeline options to run Apache Beam pipelines. To run the pipeline locally without extra setup dependencies, this example uses the `LocalDagRunner` for orchestration. The pipeline created can specify Apache Beam’s pipeline options through the `beam_pipeline_args` argument.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_local.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_local.py" tfx_execute_pipeline >}}
{{< /highlight >}}
