---
title: "About Beam ML"
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

# About Beam ML

<table>
  <tr>
    <td>
      <a>
      {{< button-pydoc path="apache_beam.ml.inference" class="RunInference" >}}
      </a>
   </td>
   <td>
      <a target="_blank" class="button"
          href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/extensions/python/transforms/RunInference.html">
        <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="30px"
            alt="Javadoc" />
      Javadoc
      </a>
    </td>
  </tr>
</table>

You can use Apache Beam to:

* Process large volumes of data, both for preprocessing and for inference.
* Experiment with your data during the exploration phase of your project and provides a seamless transition when
  upscaling your data pipelines as part of your MLOps ecosystem in a production environment.
* Run your model in production on a varying data load, both in batch and streaming.

## AI/ML workloads

You can use Apache Beam for data validation, data preprocessing, model validation, and model deployment/inference.

![Overview of AI/ML building blocks and where Apache Beam can be used](/images/ml-workflows.svg)

1. Data ingestion: Incoming new data is stored in your file system or database, or it's published to a messaging queue.
2. **Data validation**: After you receieve your data, check the quality of your data. For example, you might want to detect outliers and calculate standard deviations and class distributions.
3. **Data preprocessing**: After you validate your data, transform the data so that it is ready to use to train your model.
4. Model training: When your data is ready, you can start training your AI/ML model. This step is typically repeated multiple times, depending on the quality of your trained model.
5. Model validation: Before you deploy your new model, validate its performance and accuracy.
6. **Model deployment**: Deploy your model, using it to run inference on new or existing data.

To keep your model up to date and performing well as your data grows and evolves, run these steps multiple times. In addition, you can apply MLOps to your project to automate the AI/ML workflows throughout the model and data lifecycle. Use orchestrators to automate this flow and to handle the transition between the different building blocks in your project.

## Use RunInference

The recommended way to implement inference in Apache Beam is by using the [RunInference API](/documentation/sdks/python-machine-learning/). RunInference takes advantage of existing Apache Beam concepts, such as the `BatchElements` transform and the `Shared` class, to enable you to use models in your pipelines to create transforms optimized for machine learning inferences. The ability to create arbitrarily complex workflow graphs also allows you to build multi-model pipelines.

You can integrate your model in your pipeline by using the corresponding model handlers. A `ModelHandler` is an object that wraps the underlying model and allows you to configure its parameters. Model handlers are available for PyTorch, scikit-learn, and TensorFlow. Examples of how to use RunInference for PyTorch, scikit-learn, and TensorFlow are shown in this [notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch_tensorflow_sklearn.ipynb).

Because GPUs can process multiple computations simultaneously, they are optimized for training artificial intelligence and deep learning models. RunInference also allows you to use GPUs for significant inference speedup. An example of how to use RunInference with GPUs is demonstrated on the [RunInference metrics](/documentation/ml/runinference-metrics) page.

RunInference takes advantage of existing Apache Beam concepts, such as the `BatchElements` transform and the `Shared` class, to enable you to use models in your pipelines to create transforms optimized for machine learning inferences. The ability to create arbitrarily complex workflow graphs also allows you to build multi-model pipelines.

### BatchElements PTransform

To take advantage of the optimizations of vectorized inference that many models implement, we added the `BatchElements` transform as an intermediate step before making the prediction for the model. This transform batches elements together. The batched elements are then applied with a transformation for the particular framework of RunInference. For example, for numpy `ndarrays`, we call `numpy.stack()`,  and for torch `Tensor` elements, we call `torch.stack()`.

To customize the settings for `beam.BatchElements`, in `ModelHandler`, override the `batch_elements_kwargs` function. For example, use `min_batch_size` to set the lowest number of elements per batch or `max_batch_size` to set the highest number of elements per batch.

For more information, see the [`BatchElements` transform documentation](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements).

### Shared helper class

Using the `Shared` class within the RunInference implementation makes it possible to load the model only once per process and share it with all DoFn instances created in that process. This feature reduces memory consumption and model loading time. For more information, see the
[`Shared` class documentation](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/utils/shared.py#L20).

### Multi-model pipelines

The RunInference API can be composed into multi-model pipelines. Multi-model pipelines can be useful for A/B testing or for building out cascade models made up of models that perform tokenization, sentence segmentation, part-of-speech tagging, named entity extraction, language detection, coreference resolution, and more. For more information, see [Multi-model pipelines](https://beam.apache.org/documentation/ml/multi-model-pipelines/).

### Use pre-trained models

The section provides requirements for using pre-trained models with PyTorch, Scikit-learn, and Tensorflow.

#### PyTorch

You need to provide a path to a file that contains the model's saved weights. This path must be accessible by the pipeline. To use pre-trained models with the RunInference API and the PyTorch framework, complete the following steps:

1. Download the pre-trained weights and host them in a location that the pipeline can access.
2. Pass the path of the model weights to the PyTorch `ModelHandler` by using the following code: `state_dict_path=<path_to_weights>`.

See [this notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch.ipynb)
that illustrates running PyTorch models with Apache Beam.

#### Scikit-learn

You need to provide a path to a file that contains the pickled Scikit-learn model. This path must be accessible by the pipeline. To use pre-trained models with the RunInference API and the Scikit-learn framework, complete the following steps:

1. Download the pickled model class and host it in a location that the pipeline can access.
2. Pass the path of the model to the Sklearn `ModelHandler` by using the following code:
   `model_uri=<path_to_pickled_file>` and `model_file_type: <ModelFileType>`, where you can specify
   `ModelFileType.PICKLE` or `ModelFileType.JOBLIB`, depending on how the model was serialized.

See [this notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_sklearn.ipynb)
that illustrates running Scikit-learn models with Apache Beam.

#### TensorFlow

To use TensorFlow with the RunInference API, you have two options:

1. Use the built-in TensorFlow Model Handlers in Apache Beam SDK - `TFModelHandlerNumpy` and `TFModelHandlerTensor`.
    * Depending on the type of input for your model, use `TFModelHandlerNumpy` for `numpy` input and `TFModelHandlerTensor` for `tf.Tensor` input respectively.
    * Use tensorflow 2.7 or later.
    * Pass the path of the model to the TensorFlow `ModelHandler` by using `model_uri=<path_to_trained_model>`.
    * Alternatively, you can pass the path to saved weights of the trained model, a function to build the model using `create_model_fn=<function>`, and set the `model_type=ModelType.SAVED_WEIGHTS`.
  See [this notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow.ipynb) that illustrates running Tensorflow models with Built-in model handlers.
2. Using `tfx_bsl`.
    * Use this approach if your model input is of type `tf.Example`.
    * Use `tfx_bsl` version 1.10.0 or later.
    * Create a model handler using `tfx_bsl.public.beam.run_inference.CreateModelHandler()`.
    * Use the model handler with the [`apache_beam.ml.inference.base.RunInference`](/releases/pydoc/current/apache_beam.ml.inference.base.html) transform.
  See [this notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow.ipynb)
  that illustrates running TensorFlow models with Apache Beam and tfx-bsl.

## Automatic model refresh

To automatically update the model being used with the RunInference `PTransform` without stopping the pipeline, pass a [`ModelMetadata`](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.ModelMetadata) side input `PCollection` to the RunInference input parameter `model_metadata_pcoll`.

`ModelMetdata` is a `NamedTuple` containing:
  * `model_id`: Unique identifier for the model. This can be a file path or a URL where the model can be accessed. It is used to load the model for inference. The URL or file path must be in the compatible format so that the respective `ModelHandlers` can load the models without errors.

    For example, `PyTorchModelHandler` initially loads a model using weights and a model class. If you pass in weights from a different model class when you update the model using side inputs, the model doesn't load properly, because it expects the weights from the original model class.
  * `model_name`: Human-readable name for the model. You can use this name to identify the model in the metrics generated by the RunInference transform.

Use cases:
 * Use `WatchFilePattern` as side input to the RunInference `PTransform` to automatically update the ML model. For more information, see [Use `WatchFilePattern` as side input to auto-update ML models in RunInference](https://beam.apache.org/documentation/ml/side-input-updates).

The side input `PCollection` must follow the [`AsSingleton`](https://beam.apache.org/releases/pydoc/current/apache_beam.pvalue.html?highlight=assingleton#apache_beam.pvalue.AsSingleton) view to avoid errors.

**Note**: If the main `PCollection` emits inputs and a side input has yet to receive inputs, the main `PCollection` is buffered until there is
            an update to the side input. This could happen with global windowed side inputs with data driven triggers, such as `AfterCount` and `AfterProcessingTime`. Until the side input is updated, emit the default or initial model ID that is used to pass the respective `ModelHandler` as a side input.

## Custom Inference

The RunInference API doesn't currently support making remote inference calls using, for example, the Natural Language API or the Cloud Vision API. Therefore, in order to use these remote APIs with Apache Beam, you need to write custom inference calls. The [Remote inference in Apache Beam notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/custom_remote_inference.ipynb) shows how to implement a custom remote inference call using `beam.DoFn`. When you implement a remote inference for real life projects, consider the following factors:

* API quotas and the heavy load you might incur on your external API. To optimize the calls to an external API, you can confgure `PipelineOptions` to limit the parallel calls to the external remote API.

* Be prepared to encounter, identify, and handle failure as gracefully as possible. Use techniques like exponential backoff and dead-letter queues (unprocessed messages queues).

* When running inference with an external API, batch your input together to allow for more efficient execution.

* Consider monitoring and measuring the performance of a pipeline when deploying, because monitoring can provide insight into the status and health of the application.

### Use custom models

If you would like to use a model that isn't specified by one of the supported frameworks, the RunInference API is designed flexibly to allow you to use any custom machine learning models.
You only need to create your own `ModelHandler` or `KeyedModelHandler` with logic to load your model and use it to run the inference.

A simple example can be found in [this notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_custom_inference.ipynb).
The `load_model` method shows how to load the model using a popular `spaCy` package while `run_inference` shows how to run the inference on a batch of examples.

## Model validation

Model validation allows you to benchmark your model’s performance against a previously unseen dataset. You can extract chosen metrics, create visualizations, log metadata, and compare the performance of different models with the end goal of validating whether your model is ready to deploy. Beam provides support for running model evaluation on a TensorFlow model directly inside your pipeline.

The [ML model evaluation](/documentation/ml/model-evaluation) page shows how to integrate model evaluation as part of your pipeline by using [TensorFlow Model Analysis (TFMA)](https://www.tensorflow.org/tfx/guide/tfma).

## Related links

* [RunInference public codelab](https://colab.sandbox.google.com/github/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_basic.ipynb)
* [RunInference notebooks](https://github.com/apache/beam/tree/master/examples/notebooks/beam-ml)
* [RunInference benchmarks](http://s.apache.org/beam-community-metrics/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1)
