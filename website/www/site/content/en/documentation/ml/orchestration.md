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

## Understanding the Beam DAG


Apache Beam is an open source, unified model for defining both batch and streaming data-parallel processing pipelines. One of the central concepts to the Beam programming model is the DAG (= Directed Acyclic Graph). Each Beam pipeline is a DAG that can be constructed through the Beam SDK in your programming language of choice (from the set of supported beam SDKs). Each node of this DAG represents a processing step (PTransform) that accepts a collection of data as input (PCollection) and outputs a transformed collection of data (PCollection). The edges define how data flows through the pipeline from one processing step to another. The image below shows an example of such a pipeline.

![A standalone beam pipeline](/images/standalone-beam-pipeline.svg)

Note that simply defining a pipeline and the corresponding DAG does not mean that data will start flowing through the pipeline. To actually execute the pipeline, it has to be deployed to one of the [supported Beam runners](https://beam.apache.org/documentation/runners/capability-matrix/). These distributed processing back-ends include Apache Flink, Apache Spark and Google Cloud Dataflow. A [Direct Runner](https://beam.apache.org/documentation/runners/direct/) is also provided to execute the pipeline locally on your machine for development and debugging purposes. Make sure to check out the [runner capability matrix](https://beam.apache.org/documentation/runners/capability-matrix/) to guarantee that the chosen runner supports the data processing steps defined in your pipeline, especially when using the Direct Runner.

## Orchestrating frameworks

Successfully delivering machine learning projects is about a lot more than training a model and calling it a day. A full ML workflow will often contain a range of other steps including data ingestion, data validation, data preprocessing, model evaluation, model deployment, data drift detection, etc. Furthermore, it’s essential to keep track of metadata and artifacts from your experiments to answer important questions like:
- What data was this model trained on and with which training parameters?
- When was this model deployed and what accuracy did it get on a test dataset?
Without this knowledge at your disposal, it will become increasingly difficult to troubleshoot, monitor and improve your ML solutions as they grow in size.

The solution: MLOps. MLOps is an umbrella term used to describe best practices and guiding principles that aim to make the development and maintenance of machine learning systems seamless and efficient. Simply put, MLOps is most often about automating machine learning workflows throughout the model and data lifecycle. Popular frameworks to create these workflow DAGs are [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/introduction/), [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html) and [TFX](https://www.tensorflow.org/tfx/guide).

So what does all of this have to do with Beam? Well, since we established that Beam is a great tool for a range of ML tasks, a beam pipeline can either be used as a standalone data processing job or can be part of a larger sequence of steps in such a workflow. In the latter case, the beam DAG is just one node in the overarching DAG composed by the workflow orchestrator. This results in a DAG in a DAG, as illustrated by the example below.

![An beam pipeline as part of a larger orchestrated workflow](/images/orchestrated-beam-pipeline.svg)

It is important to understand the key difference between the Beam DAG and the orchestrating DAG. The Beam DAG processes data and passes that data between the nodes of its DAG. The focus of Beam is on parallelization and enabling both batch and streaming jobs. In contrast, the orchestration DAG schedules and monitors steps in the workflow and passed between the nodes of the DAG are execution parameters, metadata and artifacts. An example of such an artifact could be a trained model or a dataset. Such artifacts are often passed by a reference URI and not by value.

Note: TFX creates a workflow DAG, which needs an orchestrator of its own to be executed. [Natively supported orchestrators for TFX](https://www.tensorflow.org/tfx/guide/custom_orchestrator) are Airflow, Kubeflow Pipelines and, here’s the kicker, Beam itself! As mentioned by the [TFX docs](https://www.tensorflow.org/tfx/guide/beam_orchestrator):

> "Several TFX components rely on Beam for distributed data processing. In addition, TFX can use Apache Beam to orchestrate and execute the pipeline DAG. Beam orchestrator uses a different BeamRunner than the one which is used for component data processing."

Caveat: The Beam orchestrator is not meant to be a TFX orchestrator to be used in production environments. It simply enables debugging TFX pipelines locally on Beam’s DirectRunner without the need for the extra setup that is needed for Airflow or Kubeflow.

## Preprocessing example

Let’s get practical and take a look at two such orchestrated ML workflows, one with Kubeflow Pipelines (KFP) and one with Tensorflow Extended (TFX). These two frameworks achieve the same goal of creating workflows, but have their own distinct advantages and disadvantages: KFP requires you to create your workflow components from scratch and requires a user to explicitly indicate which artifacts should be passed between components and in what way. In contrast, TFX offers a number of prebuilt components and takes care of the artifact passing more implicitly. Clearly, there is a trade-off to be considered between flexibility and programming overhead when choosing between the two frameworks. We will start by looking at an example with KFP and then transition to TFX to show TFX takes care of a lot of functionality that we had to define by hand in the KFP example.

For simplicity, we will showcase workflows with only three components: data ingestion, data preprocessing and model training. Depending on the scenario, a range of extra components could be added such as model evaluation, model deployment, etc. We will focus our attention on the preprocessing component, since it showcases how to use Apache beam in an ML workflow for efficient and parallel processing of your ML data.

The dataset we will use consists of image-caption pairs, i.e. images paired with a textual caption describing the content of the image. These pairs are taken from captions subset of the [MSCOCO 2014 dataset](https://cocodataset.org/#home). This multi-modal data (image + text) gives us the opportunity to experiment with preprocessing operations for both modalities.

### Kubeflow pipelines (KFP)

In order to execute our ML workflow with KFP we must perform three steps:

1. Create the KFP components by specifying the interface to the components and by writing and containerizing the implementation of the component logic
2. Create the KFP pipeline by connecting the created components and specifying how inputs and outputs should be passed from between components and compiling the pipeline definition to a full pipeline definition.
3. Execute the KFP pipeline by submitting it to a KFP client endpoint.

The full example code can be found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/kfp)

#### Create the KFP components

This is our target file structure:

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

Let’s start with the component specifications. The full preprocessing component specification is illustrated below. The inputs are the path where the ingested dataset was saved by the ingest component and a path to a directory where the component can store artifacts. Additionally, there are some inputs that specify how and where the Beam pipeline should run. The specifications for the ingestion and train component are similar and can be found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/kfp/components/ingestion/component.yaml) and [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/kfp/components/train/component.yaml), respectively.

>Note: we are using the KFP v1 SDK, because v2 is still in [beta](https://www.kubeflow.org/docs/started/support/#application-status). The v2 SDK introduces some new options for specifying the component interface with more native support for input and output artifacts. To see how to migrate components from v1 to v2, consult the [KFP docs](https://www.kubeflow.org/docs/components/pipelines/sdk-v2/v2-component-io/).

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/component.yaml" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/component.yaml" preprocessing_component_definition >}}
{{< /highlight >}}

In this case, each component shares an identical Dockerfile but extra component-specific dependencies could be added where necessary.

{{< highlight language="Dockerfile" file="sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/Dockerfile" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/Dockerfile" component_dockerfile >}}
{{< /highlight >}}

With the component specification and containerization out of the way we can look at the actual implementation of the preprocessing component.

Since KFP provides the input and output arguments as command-line arguments, an `argumentparser` is needed.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kf/components/preprocessing/src/preprocess.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" preprocess_component_argparse >}}
{{< /highlight >}}

The implementation of the `preprocess_dataset` function contains the Beam pipeline code and the Beam pipeline options to select the desired runner. The executed preprocessing involves downloading the image bytes from their url, converting them to a Torch Tensor and resizing to the desired size. The caption undergoes a series of string manipulations to ensure that our model receives clean uniform image descriptions (Tokenization is not yet done here, but could be included here as well if the vocabulary is known). Finally each element is serialized and written to [Avro](https://avro.apache.org/docs/1.2.0/) files (Alternative files formats could be used as well, e.g. TFRecords).


{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" deploy_preprocessing_beam_pipeline >}}
{{< /highlight >}}

It also contains the necessary code to perform the component IO. First, a target path is constructed to store the preprocessed dataset based on the component input parameter `base_artifact_path` and a timestamp. Output values from components can only be returned as files so we write the value of the constructed target path to an output file that was provided by KFP to our component.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/components/preprocessing/src/preprocess.py" kfp_component_input_output >}}
{{< /highlight >}}

Since we are mainly interested in the preprocessing component to show how a Beam pipeline can be integrated into a larger ML workflow, we will not cover the implementation of the ingestion and train component in depth. Implementations of dummy components that mock their behavior are provided in the full example code.

#### Create the pipeline definition

`pipeline.py` first loads the created components from their specification `.yaml` file.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" load_kfp_components >}}
{{< /highlight >}}

After that, the pipeline is created and the required components inputs and outputs are specified manually.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" define_kfp_pipeline >}}
{{< /highlight >}}

Finally, the defined pipeline is compiled and a `pipeline.json` specification file is generated.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" compile_kfp_pipeline >}}
{{< /highlight >}}


#### Execute the KFP pipeline

Using the specification file and the snippet below with the necessary [requirements](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/kfp/requirements.txt) installed, the pipeline can now be executed. Consult the [docs](https://kubeflow-pipelines.readthedocs.io/en/latest/source/kfp.client.html#kfp.Client.run_pipeline) for more information. Note that, before executing the pipeline, a container for each component must be built and pushed to a container registry that can be accessed by your pipeline execution. Also make sure that the component specification `.yaml` files are updated to point to the correct container image.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/kfp/pipeline.py" execute_kfp_pipeline >}}
{{< /highlight >}}


### Tensorflow Extended (TFX)

The way of working for TFX is similar to the approach for KFP as illustrated above: Define the individual workflow components, connect them in a pipeline object and run the pipeline in the target environment. However, what makes TFX different is that it has already built a set of Python packages that are libraries to create workflow components. So unlike the KFP example, we do not need to start from scratch by writing and containerizing our code. What is left for the users to do is pick which of those TFX components are relevant to their specific workflow and adapt their functionality to the specific use case using the library. The image below shows the available components and their corresponding libraries. The link with Apache Beam is that TFX relies heavily on it to implement data-parallel pipelines in these libraries. This means that components created with these libraries will need to be run on one of the support Beam runners. The full example code can again be found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/ml-orchestration/tfx)


![TFX libraries and components](https://www.tensorflow.org/static/tfx/guide/images/libraries_components.png)

We will work out a small example in a similar fashion as for KFP. There we used ingestion, preprocessing and trainer components. Translating this to TFX, we will need the ExampleGen, Transform and Trainer libraries.

This time we will start by looking at the pipeline definition. Note that this looks very similar to our previous example.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_local.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_local.py" tfx_pipeline >}}
{{< /highlight >}}

We will use the same data input as last time, i.e. a couple of image-captions pairs extracted from the [MSCOCO 2014 dataset](https://cocodataset.org/#home). This time, however, in CSV format because the ExampleGen component does not by default have support for jsonlines. (The formats that are supported out of the box are listed [here](https://www.tensorflow.org/tfx/guide/examplegen#data_sources_and_formats). Alternatively, it’s possible to write a [custom ExampleGen](https://www.tensorflow.org/tfx/guide/examplegen#custom_examplegen) as well.)

Copy the snippet below to an input data csv file:

{{< highlight >}}
image_id,id,caption,image_url,image_name,image_license
318556,255,"An angled view of a beautifully decorated bathroom.","http://farm4.staticflickr.com/3133/3378902101_3c9fa16b84_z.jpg","COCO_train2014_000000318556.jpg","Attribution-NonCommercial-ShareAlike License"
476220,14,"An empty kitchen with white and black appliances.","http://farm7.staticflickr.com/6173/6207941582_b69380c020_z.jpg","COCO_train2014_000000476220.jpg","Attribution-NonCommercial License"
{{< /highlight >}}

So far, we have only imported standard TFX components and chained them together into a pipeline. Both the Transform and Trainer components have a `module_file` argument defined. That’s where we define the behavior we want from these standard components.

#### Preprocess

The Transform component searches the `module_file` for a definition of the function `preprocessing_fn`. This function is the central concept of the `tf.transform` library. As per the [TFX docs](https://www.tensorflow.org/tfx/transform/get_started#define_a_preprocessing_function):

> The preprocessing function is the most important concept of tf.Transform. The preprocessing function is a logical description of a transformation of the dataset. The preprocessing function accepts and returns a dictionary of tensors, where a tensor means Tensor or SparseTensor. There are two kinds of functions used to define the preprocessing function:
>1. Any function that accepts and returns tensors. These add TensorFlow operations to the graph that transform raw data into transformed data.
>2. Any of the analyzers provided by tf.Transform. Analyzers also accept and return tensors, but unlike TensorFlow functions, they do not add operations to the graph. Instead, analyzers cause tf.Transform to compute a full-pass operation outside of TensorFlow. They use the input tensor values over the entire dataset to generate a constant tensor that is returned as the output. For example, tft.min computes the minimum of a tensor over the dataset. tf.Transform provides a fixed set of analyzers, but this will be extended in future versions.

So our `preprocesing_fn` can contain all tf operations that accept and return tensors and also specific `tf.transform` operations. In our simple example below we use the former to convert all incoming captions to lowercase letters only, while the latter does a full pass on all the data in our dataset to compute the average length of the captions to be used for a follow-up preprocessing step.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" tfx_preprocessing_fn >}}
{{< /highlight >}}

However this function only defines the logical steps that have to be performed during preprocessing and needs a concrete implementation before it can be executed. One such implementation is provided by `tf.Transform` using Apache Beam and provides a PTransform `tft_beam.AnalyzeAndTransformDataset` to process the data. We can test this preproccesing_fn outside of the TFX Transform component using this PTransform explicitly. Calling the `processing_fn` in such a way is not necessary when using `tf.Transform` in combination with the TFX Transform component.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" tfx_analyze_and_transform >}}
{{< /highlight >}}

#### Train

Finally the Trainer component behaves in a similar way as the Transform component, but instead of looking for a `preprocessing_fn` it requires a `run_fn` function to be present in the specified `module_file`. Our simple implementation, creates a stub model using `tf.Keras` and saves the resulting model to a directory.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_utils.py" tfx_run_fn >}}
{{< /highlight >}}

#### Executing the pipeline

To launch the pipeline two configurations must be provided: The orchestrator for the TFX pipeline and the pipeline options to run Beam pipelines. In this case we use the `LocalDagRunner` for orchestration to run the pipeline locally without extra setup dependencies. Where the created pipeline can specify Beam’s pipeline options as usual through the `beam_pipeline_args` argument.

{{< highlight file="sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_local.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/ml-orchestration/tfx/coco_captions_local.py" tfx_execute_pipeline >}}
{{< /highlight >}}
