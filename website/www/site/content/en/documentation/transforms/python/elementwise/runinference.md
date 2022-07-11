---
title: "RunInference"
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

# RunInference

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.ml.inference" class="RunInference" >}}

Uses models to do local and remote inference. A `RunInference` transform uses a `PCollection` of examples to create a machine learning (ML) model. The transform outputs a `PCollection` that contains the input examples and output predictions.

You must have Apache Beam 2.40.0 or later installed to run these pipelines.

See more [RunInference API pipeline examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference).

## PyTorch dependencies

The RunInference API supports the PyTorch framework. To use PyTorch locally, first install `torch`. To install `torch`, in your terminal, run the following command:

`pip install torch==1.10.0`

If you are using pretrained models from Pytorch's `torchvision.models` [subpackage](https://pytorch.org/vision/0.12/models.html#models-and-pre-trained-weights), you also need to install `torchvision`. To install `torchvision`, in your terminal, run the following command:

`pip install torchvision`

If you are using pretrained models from Hugging Face's [`transformers` package](https://huggingface.co/docs/transformers/index), you need to install `transformers`. To install `transformers`, in your terminal, run the following command:

`pip install transformers`

For information about installing the `torch` dependency on a distributed runner such as Dataflow, see the [PyPI dependency instructions](/documentation/sdks/python-pipeline-dependencies/#pypi-dependencies).

RunInference uses dynamic batching. However, the RunInference API cannot batch tensor elements of different sizes, because `torch.stack()` expects tensors of the same length. If you provide images of different sizes or word embeddings of different lengths, errors might occur.

To avoid this issue:

1. Either use elements that have the same size, or resize image inputs and word embeddings to make them 
the same size. Depending on the language model and encoding technique, this option might not be available. 
2. Disable batching by overriding the `batch_elements_kwargs` function in your ModelHandler and setting the maximum batch size (`max_batch_size`) to one: `max_batch_size=1`. For more information, see
[BatchElements PTransforms](/documentation/sdks/python-machine-learning/#batchelements-ptransform).

## Examples

In the following examples, we explore how to create pipelines that use the Beam RunInference API to make predictions based on models.

### Example 1: Image classification

In this example, we create a pipeline that performs image classification using the `mobilenet_v2` architecture.

This pipeline reads your set of images, performs preprocessing, passes the images to the PyTorch implementation of RunInference, and then writes the predictions to a text file.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py"
  class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py" images >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference_test.py" images >}}
{{< /highlight >}}

### Example 2: MNIST digit classification

In this example, we create a pipeline that performs image classification on handwritten digits from the
[MNIST](https://en.wikipedia.org/wiki/MNIST_database) database.

This pipeline reads a text file that contains data in comma separated integers. The first
column contains the true label and the following integers in the row are pixel values. The transform processes the data and then uses a model trained on the MNIST data to perform the prediction. The pipeline writes the prediction to an output file.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py"
  notebook="examples/notebooks/documentation/transforms/python/elementwise/pardo" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py" digits >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py" digits >}}
{{< /highlight >}}


## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.ml.inference" class="RunInference" >}}
