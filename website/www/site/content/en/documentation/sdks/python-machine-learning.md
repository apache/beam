---
type: languages
title: "Apache Beam Python Machine Learning"
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

# Machine Learning with Python

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


You can use Apache Beam with the RunInference API to use machine learning (ML) models to do local and remote inference with batch and streaming pipelines. Starting with Apache Beam 2.40.0, PyTorch and Scikit-learn frameworks are supported. Tensorflow models are supported through tfx-bsl.

You can create multiple types of transforms using the RunInference API: the API takes multiple types of setup parameters from model handlers, and the parameter type determines the model implementation.

For more infomation about machine learning with Apache Beam, see:

* [Get started with AI/ML](/documentation/ml/overview)
* [About Beam ML](/documentation/ml/about-ml)
* [RunInference notebooks](https://github.com/apache/beam/tree/master/examples/notebooks/beam-ml)

## Why use the RunInference API?

RunInference takes advantage of existing Apache Beam concepts, such as the `BatchElements` transform and the `Shared` class, to enable you to use models in your pipelines to create transforms optimized for machine learning inferences. The ability to create arbitrarily complex workflow graphs also allows you to build multi-model pipelines.

### BatchElements PTransform

To take advantage of the optimizations of vectorized inference that many models implement, we added the `BatchElements` transform as an intermediate step before making the prediction for the model. This transform batches elements together. The batched elements are then applied with a transformation for the particular framework of RunInference. For example, for numpy `ndarrays`, we call `numpy.stack()`,  and for torch `Tensor` elements, we call `torch.stack()`.

To customize the settings for `beam.BatchElements`, in `ModelHandler`, override the `batch_elements_kwargs` function. For example, use `min_batch_size` to set the lowest number of elements per batch or `max_batch_size` to set the highest number of elements per batch.

For more information, see the [`BatchElements` transform documentation](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements).

### Shared helper class

Using the `Shared` class within the RunInference implementation makes it possible to load the model only once per process and share it with all DoFn instances created in that process. This feature reduces memory consumption and model loading time. For more information, see the
[`Shared` class documentation](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/utils/shared.py#L20).

### Multi-model pipelines

The RunInference API can be composed into multi-model pipelines. Multi-model pipelines can be useful for A/B testing or for building out cascade models made up of models that perform tokenization, sentence segmentation, part-of-speech tagging, named entity extraction, language detection, coreference resolution, and more.
For more information about multi-model pipelines, see
[Multi-model pipelines](/documentation/ml/multi-model-pipelines/).

## Modify a Python pipeline to use an ML model

To use the RunInference transform, add the following code to your pipeline:

```
from apache_beam.ml.inference.base import RunInference
with pipeline as p:
   predictions = ( p |  'Read' >> beam.ReadFromSource('a_source')
                     | 'RunInference' >> RunInference(<model_handler>)
```
Where `model_handler` is the model handler setup code.

To import models, you need to configure a `ModelHandler` object that wraps the underlying model. Which `ModelHandler` you import depends on the framework and type of data structure that contains the inputs. The `ModelHandler` also allows you to set environment variables needed for inference via the `env_vars` keyword argument. The following examples show some ModelHandlers that you might want to import.

```
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerPandas
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from tfx_bsl.public.beam.run_inference import CreateModelHandler
```

## Use pre-trained models

The section provides requirements for using pre-trained models with PyTorch and Scikit-learn

### PyTorch

You need to provide a path to a file that contains the model's saved weights. This path must be accessible by the pipeline. To use pre-trained models with the RunInference API and the PyTorch framework, complete the following steps:

1. Download the pre-trained weights and host them in a location that the pipeline can access.
2. Pass the path of the model weights to the PyTorch `ModelHandler` by using the following code: `state_dict_path=<path_to_weights>`.

See [this notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch.ipynb)
that illustrates running PyTorch models with Apache Beam.

### Scikit-learn

You need to provide a path to a file that contains the pickled Scikit-learn model. This path must be accessible by the pipeline. To use pre-trained models with the RunInference API and the Scikit-learn framework, complete the following steps:

1. Download the pickled model class and host it in a location that the pipeline can access.
2. Pass the path of the model to the Sklearn `ModelHandler` by using the following code:
   `model_uri=<path_to_pickled_file>` and `model_file_type: <ModelFileType>`, where you can specify
   `ModelFileType.PICKLE` or `ModelFileType.JOBLIB`, depending on how the model was serialized.

See [this notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_sklearn.ipynb)
that illustrates running Scikit-learn models with Apache Beam.

### TensorFlow

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

## Use custom models

If you would like to use a model that isn't specified by one of the supported frameworks, the RunInference API is designed flexibly to allow you to use any custom machine learning models.
You only need to create your own `ModelHandler` or `KeyedModelHandler` with logic to load your model and use it to run the inference.

A simple example can be found in [this notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_custom_inference.ipynb).
The `load_model` method shows how to load the model using a popular `spaCy` package while `run_inference` shows how to run the inference on a batch of examples.

## Use multiple models

You can also use the RunInference transform to add multiple inference models to your Python pipeline.

### A/B Pattern

```
with pipeline as p:
   data = p | 'Read' >> beam.ReadFromSource('a_source')
   model_a_predictions = data | RunInference(<model_handler_A>)
   model_b_predictions = data | RunInference(<model_handler_B>)
```

Where `model_handler_A` and `model_handler_B` are the model handler setup code.

### Cascade Pattern

```
with pipeline as p:
   data = p | 'Read' >> beam.ReadFromSource('a_source')
   model_a_predictions = data | RunInference(<model_handler_A>)
   model_b_predictions = model_a_predictions | beam.Map(some_post_processing) | RunInference(<model_handler_B>)
```

Where `model_handler_A` and `model_handler_B` are the model handler setup code.

### Use Resource Hints for Different Model Requirements

When using multiple models in a single pipeline, different models may have different memory or worker SKU requirements.
Resource hints allow you to provide information to a runner about the compute resource requirements for each step in your
pipeline.

For example, the following snippet extends the previous cascade pattern with hints for each RunInference call
to specify RAM and hardware accelerator requirements:

```
with pipeline as p:
   data = p | 'Read' >> beam.ReadFromSource('a_source')
   model_a_predictions = data | RunInference(<model_handler_A>).with_resource_hints(min_ram="20GB")
   model_b_predictions = model_a_predictions
      | beam.Map(some_post_processing)
      | RunInference(<model_handler_B>).with_resource_hints(
         min_ram="4GB",
         accelerator="type:nvidia-tesla-k80;count:1;install-nvidia-driver")
```

For more information on resource hints, see [Resource hints](/documentation/runtime/resource-hints/).

## RunInference Patterns

This section suggests patterns and best practices that you can use to make your inference pipelines simpler,
more robust, and more efficient.

### Use a keyed ModelHandler

If a key is attached to the examples, wrap the `KeyedModelHandler` around the `ModelHandler` object:

```
from apache_beam.ml.inference.base import KeyedModelHandler
keyed_model_handler = KeyedModelHandler(PytorchModelHandlerTensor(...))
with pipeline as p:
   data = p | beam.Create([
      ('img1', torch.tensor([[1,2,3],[4,5,6],...])),
      ('img2', torch.tensor([[1,2,3],[4,5,6],...])),
      ('img3', torch.tensor([[1,2,3],[4,5,6],...])),
   ])
   predictions = data | RunInference(keyed_model_handler)
```

If you are unsure if your data is keyed, you can also use `MaybeKeyedModelHandler`.

For more information, see [`KeyedModelHander`](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.KeyedModelHandler).

### Use the `PredictionResult` object

When doing a prediction in Apache Beam, the output `PCollection` includes both the keys of the input examples and the inferences. Including both these items in the output allows you to find the input that determined the predictions.

The `PredictionResult` is a `NamedTuple` object that contains both the input and the inferences, named  `example` and  `inference`, respectively. When keys are passed with the input data to the RunInference transform, the output `PCollection` returns a `Tuple[str, PredictionResult]`, which is the key and the `PredictionResult` object. Your pipeline interacts with a `PredictionResult` object in steps after the RunInference transform.

```
class PostProcessor(beam.DoFn):
    def process(self, element: Tuple[str, PredictionResult]):
       key, prediction_result = element
       inputs = prediction_result.example
       predictions = prediction_result.inference

       # Post-processing logic
       result = ...

       yield (key, result)

with pipeline as p:
    output = (
        p | 'Read' >> beam.ReadFromSource('a_source')
                | 'PyTorchRunInference' >> RunInference(<keyed_model_handler>)
                | 'ProcessOutput' >> beam.ParDo(PostProcessor()))
```

If you need to use this object explicitly, include the following line in your pipeline to import the object:

```
from apache_beam.ml.inference.base import PredictionResult
```

For more information, see the [`PredictionResult` documentation](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/ml/inference/base.py#L65).

### Automatic model refresh
To automatically update the models used with the RunInference `PTransform` without stopping the Beam pipeline, pass a [`ModelMetadata`](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.ModelMetadata) side input `PCollection` to the RunInference input parameter `model_metadata_pcoll`.

`ModelMetdata` is a `NamedTuple` containing:
  * `model_id`: Unique identifier for the model. This can be a file path or a URL where the model can be accessed. It is used to load the model for inference. The URL or file path must be in the compatible format so that the respective `ModelHandlers` can load the models without errors.

    For example, `PyTorchModelHandler` initially loads a model using weights and a model class. If you pass in weights from a different model class when you update the model using side inputs, the model doesn't load properly, because it expects the weights from the original model class.
  * `model_name`: Human-readable name for the model. You can use this name to identify the model in the metrics generated by the RunInference transform.

Use cases:
 * Use `WatchFilePattern` as side input to the RunInference `PTransform` to automatically update the ML model. For more information, see [Use `WatchFilePattern` as side input to auto-update ML models in RunInference](https://beam.apache.org/documentation/ml/side-input-updates).

The side input `PCollection` must follow the [`AsSingleton`](https://beam.apache.org/releases/pydoc/current/apache_beam.pvalue.html?highlight=assingleton#apache_beam.pvalue.AsSingleton) view to avoid errors.

**Note**: If the main `PCollection` emits inputs and a side input has yet to receive inputs, the main `PCollection` is buffered until there is
            an update to the side input. This could happen with global windowed side inputs with data driven triggers, such as `AfterCount` and `AfterProcessingTime`. Until the side input is updated, emit the default or initial model ID that is used to pass the respective `ModelHandler` as a side input.

### Preprocess and postprocess your records

With RunInference, you can add preprocessing and postprocessing operations to your transform.
To apply preprocessing operations, use `with_preprocess_fn` on your model handler:

```
inference = pcoll | RunInference(model_handler.with_preprocess_fn(lambda x : do_something(x)))
```

To apply postprocessing operations, use `with_postprocess_fn` on your model handler:

```
inference = pcoll | RunInference(model_handler.with_postprocess_fn(lambda x : do_something_to_result(x)))
```

You can also chain multiple pre- and postprocessing operations:

```
inference = pcoll | RunInference(
    model_handler.with_preprocess_fn(
      lambda x : do_something(x)
    ).with_preprocess_fn(
      lambda x : do_something_else(x)
    ).with_postprocess_fn(
      lambda x : do_something_after_inference(x)
    ).with_postprocess_fn(
      lambda x : do_something_else_after_inference(x)
    ))
```

The preprocessing function is run before batching and inference. This function maps your input `PCollection`
to the base input type of the model handler. If you apply multiple preprocessing functions, they run on your original
`PCollection` in the order of last applied to first applied.

The postprocessing function runs after inference. This function maps the output type of the base model handler
to your desired output type. If you apply multiple postprocessing functions, they run on your original
inference result in the order of first applied to last applied.

### Handle errors while using RunInference

To handle errors robustly while using RunInference, you can use a _dead-letter queue_. The dead-letter queue outputs failed records into a separate `PCollection` for further processing.
This `PCollection` can then be analyzed and sent to a storage system, where it can be reviewed and resubmitted to the pipeline, or discarded.
RunInference has built-in support for dead-letter queues. You can use a dead-letter queue by applying `with_exception_handling` to your RunInference transform:

```
main, other = pcoll | RunInference(model_handler).with_exception_handling()
other.failed_inferences | beam.Map(print) # insert logic to handle failed records here
```

You can also apply this pattern to RunInference transforms with associated pre- and postprocessing operations:

```
main, other = pcoll | RunInference(model_handler.with_preprocess_fn(f1).with_postprocess_fn(f2)).with_exception_handling()
other.failed_preprocessing[0] | beam.Map(print) # handles failed preprocess operations, indexed in the order in which they were applied
other.failed_inferences | beam.Map(print) # handles failed inferences
other.failed_postprocessing[0] | beam.Map(print) # handles failed postprocess operations, indexed in the order in which they were applied
```

### Run inference from a Java pipeline

The RunInference API is available with the Beam Java SDK versions 2.41.0 and later through Apache Beam's [Multi-language Pipelines framework](/documentation/programming-guide/#multi-language-pipelines). For information about the Java wrapper transform, see [RunInference.java](https://github.com/apache/beam/blob/master/sdks/java/extensions/python/src/main/java/org/apache/beam/sdk/extensions/python/transforms/RunInference.java). To try it out, see the [Java Sklearn Mnist Classification example](https://github.com/apache/beam/tree/master/examples/multi-language). Additionally, see [Using RunInference from Java SDK](https://beam.apache.org/documentation/ml/multi-language-inference/) for an example of a composite Python transform that uses the RunInference API along with preprocessing and postprocessing from a Beam Java SDK pipeline.

## Troubleshooting

If you run into problems with your pipeline or job, this section lists issues that you might encounter and provides suggestions for how to fix them.

### Unable to batch tensor elements

RunInference uses dynamic batching. However, the RunInference API cannot batch tensor elements of different sizes, so samples passed to the RunInferene transform must be the same dimension or length. If you provide images of different sizes or word embeddings of different lengths, the following error might occur:

`
File "/beam/sdks/python/apache_beam/ml/inference/pytorch_inference.py", line 232, in run_inference
batched_tensors = torch.stack(key_to_tensor_list[key])
RuntimeError: stack expects each tensor to be equal size, but got [12] at entry 0 and [10] at entry 1 [while running 'PyTorchRunInference/ParDo(_RunInferenceDoFn)']
`

To avoid this issue, either use elements of the same size, or disable batching.

**Option 1: Use elements of the same size**

Use elements of the same size or resize the inputs. For computer vision applications, resize image inputs so that they have the same dimensions. For natural language processing (NLP) applications that have text of varying length, resize the text or word embeddings to make them the same length. When working with texts of varying length, resizing might not be possible. In this scenario, you could disable batching (see option 2).

**Option 2: Disable batching**

Disable batching by overriding the `batch_elements_kwargs` function in your ModelHandler and setting the maximum batch size (`max_batch_size`) to one: `max_batch_size=1`. For more information, see
[BatchElements PTransforms](/documentation/ml/about-ml/#batchelements-ptransform). For an example, see our [language modeling example](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_language_modeling.py).

## Related links

* [RunInference transforms](/documentation/transforms/python/elementwise/runinference)
* [RunInference API pipeline examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference)
* [RunInference public codelab](https://colab.sandbox.google.com/github/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_basic.ipynb)
* [RunInference notebooks](https://github.com/apache/beam/tree/master/examples/notebooks/beam-ml)
* [RunInference benchmarks](http://s.apache.org/beam-community-metrics/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1)

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
