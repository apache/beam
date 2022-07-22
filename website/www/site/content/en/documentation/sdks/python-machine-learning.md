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

# Machine Learning

You can use Apache Beam with the RunInference API to use machine learning (ML) models to do local and remote inference with batch and streaming pipelines. Starting with Apache Beam 2.40.0, PyTorch and Scikit-learn frameworks are supported. You can create multiple types of transforms using the RunInference API: the API takes multiple types of setup parameters from model handlers, and the parameter type determines the model implementation.

## Why use the RunInference API?

RunInference takes advantage of existing Apache Beam concepts, such as the the `BatchElements` transform and the `Shared` class, to enable you to use models in your pipelines to create transforms optimized for machine learning inferences. The ability to create arbitrarily complex workflow graphs also allows you to build multi-model pipelines.

### BatchElements PTransform

To take advantage of the optimizations of vectorized inference that many models implement, we added the `BatchElements` transform as an intermediate step before making the prediction for the model. This transform batches elements together. The batched elements are then applied with a transformation for the particular framework of RunInference. For example, for numpy `ndarrays`, we call `numpy.stack()`,  and for torch `Tensor` elements, we call `torch.stack()`.

To customize the settings for `beam.BatchElements`, in `ModelHandler`, override the `batch_elements_kwargs` function. For example, use `min_batch_size` to set the lowest number of elements per batch or `max_batch_size` to set the highest number of elements per batch.

For more information, see the [`BatchElements` transform documentation](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements).

### Shared helper class

Using the `Shared` class within RunInference implementation allows us to load the model only once per process and share it with all DoFn instances created in that process. This feature reduces memory consumption and model loading time. For more information, see the
[`Shared` class documentation](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/utils/shared.py#L20).

### Multi-model pipelines

The RunInference API can be composed into multi-model pipelines. Multi-model pipelines can be useful for A/B testing or for building out ensembles that are comprised of models that perform tokenization, sentence segmentation, part-of-speech tagging, named entity extraction, language detection, coreference resolution, and more.

## Modify a pipeline to use an ML model

To use the RunInference transform, add the following code to your pipeline:

```
from apache_beam.ml.inference.base import RunInference
with pipeline as p:
   predictions = ( p |  'Read' >> beam.ReadFromSource('a_source')
                     | 'RunInference' >> RunInference(<model_handler>)
```
Where `model_handler` is the model handler setup code.

To import models, you need to configure a `ModelHandler` object that wraps the underlying model. Which `ModelHandler` you import depends on the framework and type of data structure that contains the inputs. The following examples show some ModelHandlers that you might want to import.

```
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerPandas
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
```
### Use pre-trained models

The section provides requirements for using pre-trained models with PyTorch and Scikit-learn

#### PyTorch

You need to provide a path to a file that contains the model saved weights. This path must be accessible by the pipeline. To use pre-trained models with the RunInference API and the PyTorch framework, complete the following steps:

1. Download the pre-trained weights and host them in a location that the pipeline can access.
2. Pass the path of the model weights to the PyTorch `ModelHandler` by using the following code: `state_dict_path=<path_to_weights>`.

#### Scikit-learn

You need to provide a path to a file that contains the pickled Scikit-learn model. This path must be accessible by the pipeline. To use pre-trained models with the RunInference API and the Scikit-learn framework, complete the following steps:

1. Download the pickled model class and host it in a location that the pipeline can access.
2. Pass the path of the model to the Sklearn `ModelHandler` by using the following code:
   `model_uri=<path_to_pickled_file>` and `model_file_type: <ModelFileType>`, where you can specify
   `ModelFileType.PICKLE` or `ModelFileType.JOBLIB`, depending on how the model was serialized.

### Use multiple models

You can also use the RunInference transform to add multiple inference models to your pipeline.

#### A/B Pattern

```
with pipeline as p:
   data = p | 'Read' >> beam.ReadFromSource('a_source')
   model_a_predictions = data | RunInference(<model_handler_A>)
   model_b_predictions = data | RunInference(<model_handler_B>)
```

Where `model_handler_A` and `model_handler_B` are the model handler setup code.

#### Ensemble Pattern

```
with pipeline as p:
   data = p | 'Read' >> beam.ReadFromSource('a_source')
   model_a_predictions = data | RunInference(<model_handler_A>)
   model_b_predictions = model_a_predictions | beam.Map(some_post_processing) | RunInference(<model_handler_B>)
```

Where `model_handler_A` and `model_handler_B` are the model handler setup code.

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
   predictions = data | RunInference(KeyedModelHandler)
```

### Use the PredictionResults object

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

## Run a machine learning pipeline

For detailed instructions explaining how to build and run a pipeline that uses ML models, see the
[Example RunInference API pipelines](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference) on GitHub.

## Troubleshooting

If you run into problems with your pipeline or job, this section lists issues that you might encounter and provides suggestions for how to fix them.

### Incorrect inferences in the PredictionResult object

In some cases, the `PredictionResults` output might not include the correct predictions in the `inferences` field. This issue occurs when you use a model whose inferences return a dictionary that maps keys to predictions and other metadata. An example return type is `Dict[str, Tensor]`.

The RunInference API currently expects outputs to be an `Iterable[Any]`. Example return types are `Iterable[Tensor]` or `Iterable[Dict[str, Tensor]]`. When RunInference zips the inputs with the predictions, the predictions iterate over the dictionary keys instead of the batch elements. The result is that the key name is preserved but the prediction tensors are discarded. For more information, see the [Pytorch RunInference PredictionResult is a Dict](https://github.com/apache/beam/issues/22240) issue in the Apache Beam GitHub project.

To work with the current RunInference implementation, you can create a wrapper class that overrides the `model(input)` call. In PyTorch, for example, your wrapper would override the `forward()` function and return an output with the appropriate format of `List[Dict[str, torch.Tensor]]`. For more information, see our [HuggingFace language modeling example](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_language_modeling.py#L49).

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
[BatchElements PTransforms](/documentation/sdks/python-machine-learning/#batchelements-ptransform). For an example, see our [language modeling example](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_language_modeling.py).

## Related links

* [RunInference transforms](/documentation/transforms/python/elementwise/runinference)
* [RunInference API pipeline examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference)
* [apache_beam.ml.inference package](/releases/pydoc/current/apache_beam.ml.inference.html#apache_beam.ml.inference.RunInference)