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

RunInference leverages existing Apache Beam concepts, such as the the `BatchElements` transform and the `Shared` class, and it allows you to build multi-model pipelines. In addition, the RunInference API allows you to find the input that determined the prediction without returning to the full input data.

### BatchElements PTransform

To take advantage of the optimizations of vectorized inference that many models implement, we added the `BatchElements` transform as an intermediate step before making the prediction for the model. This transform batches elements together. The resulting batch is used to make the appropriate transformation for the particular framework of RunInference. For example, for numpy `ndarrays`, we call `numpy.stack()`,  and for torch `Tensor` elements, we call `torch.stack()`.

To customize the settings for `beam.BatchElements`, in `ModelHandler`, override the `batch_elements_kwargs` function. For example, use `min_batch_size` to set the lowest number of elements per batch or `max_batch_size` to set the highest number of elements per batch.

For more information, see the [`BatchElements` transform documentation](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements).

### Shared helper class

Instead of loading a model for each thread in a worker, we use the `Shared` class, which allows us to load one model that is shared across all threads of each worker in a DoFn. For more information, see the
[`Shared` class documentation](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/utils/shared.py#L20).

### Multi-model pipelines

The RunInference API allows you to build complex multi-model pipelines with minimum effort. Multi-model pipelines are useful for A/B testing and for building out ensembles for tokenization, sentence segmentation, part-of-speech tagging, named entity extraction, language detection, coreference resolution, and more.

### Prediction results

When doing a prediction in Apache Beam, the output `PCollection` includes both the keys of the input examples and the inferences. Including both these items in the output allows you to find the input that determined the predictions without returning the full input data.

For more information, see the [`PredictionResult` documentation](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/ml/inference/base.py#L65). 

## Modify a pipeline to use an ML model

To use the RunInference transform, you add a single line of code in your pipeline:

```
from apache_beam.ml.inference.base import RunInference
 
with pipeline as p:
   predictions = ( p | beam.ReadFromSource('a_source')   
    | RunInference(configuration)))
```

To import models, you need to wrap them around a `ModelHandler object`. Add one or more of the following lines of code, depending on the framework and type of data structure that holds the data:

```
from apache_beam.ml.inference.pytorch_inference import SklearnModelHandlerNumpy
```
```
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
```
```
from apache_beam.ml.inference.pytorch_inference import SklearnModelHandlerPandas
```
```
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
```
### Use pre-trained models

You need to provide a path to the model weights that's accessible by the pipeline. To use pre-trained models with the RunInference API and the PyTorch framework, complete the following steps:

1. Download the pre-trained weights and host them in a location that the pipeline can access.
2. Pass the hosted path of the model to the PyTorch `model_handler` by using the following code: `state_dict_path=<path_to_weights>`.

### Use multiple inference models

You can also use the RunInference transform to add multiple inference models to your pipeline.

#### A/B Pattern

```
with pipeline as p:
   data = p | 'Read' >> beam.ReadFromSource('a_source') 
   model_a_predictions = data | RunInference(ModelHandlerA)
   model_b_predictions = data | RunInference(ModelHandlerB)
```

#### Ensemble Pattern

```
with pipeline as p:
   data = p | 'Read' >> beam.ReadFromSource('a_source') 
   model_a_predictions = data | RunInference(ModelHandlerA)
   model_b_predictions = data | RunInference(ModelHandlerB)
```

### Use a key handler

If a key is attached to the examples, use the `KeyedModelHandler`:

```
from apache_beam.ml.inference.base import KeyedModelHandler
 
keyed_model_handler = KeyedModelHandler(PytorchModelHandlerTensor(...))
 
with pipeline as p:
   data = p | beam.Create([
      ('img1', np.array[[1,2,3],[4,5,6],...]),
      ('img2', np.array[[1,2,3],[4,5,6],...]),
      ('img3', np.array[[1,2,3],[4,5,6],...]),
   ])
   predictions = data | RunInference(keyed_model_handler)
```

### Use the prediction results object

The `PredictionResult` is a `NamedTuple` object that contains both the input and the inferences, named  `example` and  `inference`, respectively. Your pipeline interacts with a `PredictionResult` object in steps after the RunInference transform.

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
                | 'PyTorchRunInference' >> RunInference(KeyedModelHandler)
                | 'ProcessOutput' >> beam.ParDo(PostProcessor()))
```

If you need to use this object explicitly, include the following line in your pipeline to import the object:

```
from apache_beam.ml.inference.base import PredictionResult
```

## Run a machine learning pipeline

For detailed instructions explaining how to build and run a pipeline that uses ML models, see the
[Example RunInference API pipelines](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference) on GitHub.

## Troubleshooting

If you run into problems with your pipeline or job, this section lists issues that you might encounter and provides suggestions for how to fix them.

### Prediction results missing

When you use a dictionary of tensors, the output might not include the prediction results. This issue occurs because the RunInference API supports tensors but not dictionaries of tensors.