---
title: "Preprocess data"
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

# Preprocess data with MLTransform

This page explains how to use the `MLTransform` class to preprocess data for machine learning (ML)
workflows. Apache Beam provides a set of data processing transforms for
preprocessing data for training and inference. The `MLTransform` class wraps the
various transforms in one class, simplifying your workflow. For a full list of
available transforms, see the [Transforms](#transforms) section on this page.

## Why use MLTransform {#use-mltransform}

-   With `MLTransform`, you can use the same preprocessing steps for both
    training and inference, which ensures consistent results.
-   Generate [embeddings](https://en.wikipedia.org/wiki/Embedding) on text data using large language models (LLMs).
-   `MLTransform` can do a full pass on the dataset, which is useful when
    you need to transform a single element only after analyzing the entire
    dataset. For example, with `MLTransform`, you can complete the following tasks:
    -   Normalize an input value by using the minimum and maximum value
        of the entire dataset.
    -   Convert `floats` to `ints` by assigning them buckets, based on
        the observed data distribution.
    -   Convert `strings` to `ints` by generating vocabulary over the
        entire dataset.
    -   Count the occurrences of words in all the documents to calculate
        [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)
        weights.
## Support and limitations {#support}

-   Available in the Apache Beam Python SDK versions 2.53.0 and later.
-   Supports Python 3.8, 3.9, 3.10, and 3.11
-   Only available for pipelines that use [default windows](/documentation/programming-guide/#single-global-window).

## Transforms {#transforms}

You can use `MLTransform` to generate text embeddings and to perform various data processing transforms.

### Text embedding transforms

You can use `MLTranform` to generate embeddings that you can use to push data into vector databases or to run inference.

{{< table >}}
| Transform name | Description |
| ------- | ---------------|
| SentenceTransformerEmbeddings | Uses the Hugging Face [`sentence-transformers`](https://huggingface.co/sentence-transformers) models to generate text embeddings.
| VertexAITextEmbeddings | Uses models from the [the Vertex AI text-embeddings API](https://cloud.google.com/vertex-ai/docs/generative-ai/embeddings/get-text-embeddings) to generate text embeddings.
{{< /table >}}


### Data processing transforms that use TFT

The following set of transforms available in the `MLTransform` class come from
the TensorFlow Transforms (TFT) library. TFT offers specialized processing
modules for machine learning tasks. For information about these transforms, see
[Module:tft](https://www.tensorflow.org/tfx/transform/api_docs/python/tft) in the
TensorFlow documentation.

{{< table >}}
| Transform name | Description |
| ------- | ---------------|
| ApplyBuckets | See [`tft.apply_buckets`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/apply_buckets) in the TensorFlow documentation. |
| ApplyBucketsWithInterpolation | See [`tft.apply_buckets_with_interpolation`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/apply_buckets_with_interpolation) in the TensorFlow documentation. |
| BagOfWords | See [`tft.bag_of_words`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/bag_of_words) in the TensorFlow documentation. |
| Bucketize | See [`tft.bucketize`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/bucketize) in the TensorFlow documentation. |
| ComputeAndApplyVocabulary | See [`tft.compute_and_apply_vocabulary`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/compute_and_apply_vocabulary) in the TensorFlow documentation. |
| DeduplicateTensorPerRow | See [`tft.deduplicate_tensor_per_row`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/deduplicate_tensor_per_row) in the TensorFlow documentation. |
| HashStrings | See [`tft.hash_strings`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/hash_strings) in the TensorFlow documentation. |
| NGrams | See [`tft.ngrams`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/ngrams) in the TensorFlow documentation. |
| ScaleByMinMax | See [`tft.scale_by_min_max`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/scale_by_min_max) in the TensorFlow documentation. |
| ScaleTo01 | See [`tft.scale_to_0_1`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/scale_to_0_1) in the TensorFlow documentation. |
| ScaleToGaussian | See [`tft.scale_to_gaussian`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/scale_to_gaussian) in the TensorFlow documentation. |
| ScaleToZScore | See [`tft.scale_to_z_score`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/scale_to_z_score) in the TensorFlow documentation. |
| TFIDF | See [`tft.tfidf`](https://www.tensorflow.org/tfx/transform/api_docs/python/tft/tfidf) in the TensorFlow documentation. |:
{{< /table >}}

## I/O requirements {#io}

-   Input to the `MLTransform` class must be a dictionary.
-   `MLTransform` outputs a Beam `Row` object with transformed elements.
-   The output `PCollection` is a schema `PCollection`. The output schema
    contains the transformed columns.

## Artifacts {#artifacts}

Artifacts are additional data elements created by data transformations.
Examples of artifacts are the minimum and maximum values from a `ScaleTo01`
transformation, or the mean and variance from a `ScaleToZScore`
transformation.

In the `MLTransform` class, the `write_artifact_location` and the
`read_artifact_location` parameters determine
whether the `MLTransform` class creates artifacts or retrieves
artifacts.

### Write mode {#write-mode}

When you use the `write_artifact_location` parameter, the `MLTransform` class runs the
specified transformations on the dataset and then creates artifacts from these
transformations. The artifacts are stored in the location that you specify in
the `write_artifact_location` parameter.

Write mode is useful when you want to store the results of your transformations
for future use. For example, if you apply the same transformations on a
different dataset, use write mode to ensure that the transformation parameters
remain consistent.

The following examples demonstrate how write mode works.

-   The `ComputeAndApplyVocabulary` transform generates a vocabulary file that contains the
    vocabulary generated over the entire dataset. The vocabulary file is stored in
    the location specified by the `write_artifact_location` parameter value.
    The `ComputeAndApplyVocabulary`
    transform outputs the indices of the vocabulary to the vocabulary file.
-   The `ScaleToZScore` transform calculates the mean and variance over the entire dataset
    and then normalizes the entire dataset using the mean and variance.
    When you use the `write_artifact_location` parameter, these
    values are stored as a `tensorflow` graph in the location specified by
    the `write_artifact_location` parameter value. You can reuse the values in read mode
    to ensure that future transformations use the same mean and variance for normalization.

### Read mode {#read-mode}

When you use the `read_artifact_location` parameter, the `MLTransform` class expects the
artifacts to exist in the value provided in the `read_artifact_location` parameter.
In this mode, `MLTransform` retrieves the artifacts and uses them in the
transform. Because the transformations are stored in the artifacts when you use
read mode, you don't need to specify the transformations.

### Artifact workflow {#artifact-workflow}

The following scenario provides an example use case for artifacts.

Before training a machine learning model, you use `MLTransform` with the
`write_artifact_location` parameter.
When you run `MLTransform`, it applies transformations that preprocess the
dataset. The transformation produces artifacts that are stored in the location
specified by the `write_artifact_location` parameter value.

After preprocessing, you use the transformed data to train the machine learning
model.

After training, you run inference. You use new test data and use the
`read_artifact_location` parameter. By using this setting, you ensure that the test
data undergoes the same preprocessing steps as the training data. In read
mode, running `MLTransform` fetches the transformation artifacts from the
location specified in the `read_artifact_location` parameter value.
`MLTransform` applies these artifacts to the test data.

This workflow provides consistency in preprocessing steps for both training and
test data. This consistency ensures that the model can accurately evaluate the
test data and maintain the integrity of the model's performance.

## Preprocess data with MLTransform {#use-mltransform}

To use the `MLTransform` transform to preprocess data, add the following code to
your pipeline:

```
  import apache_beam as beam
  from apache_beam.ml.transforms.base import MLTransform
  from apache_beam.ml.transforms.tft import <TRANSFORM_NAME>
  import tempfile

  data = [
      {
          <DATA>
      },
  ]

  artifact_location = tempfile.mkdtemp()
  <TRANSFORM_FUNCTION_NAME> = <TRANSFORM_NAME>(columns=['x'])

  with beam.Pipeline() as p:
    transformed_data = (
        p
        | beam.Create(data)
        | MLTransform(write_artifact_location=artifact_location).with_transform(
            <TRANSFORM_FUNCTION_NAME>)
        | beam.Map(print))
```

Replace the following values:

-   TRANSFORM_NAME: The name of the [transform](#transforms) to use.
-   DATA: The input data to transform.
-   TRANSFORM_FUNCTION_NAME: The name that you assign to your transform
    function in your code.

For more examples, see
[MLTransform for data processing](/documentation/transforms/python/elementwise/mltransform)
in the [transform catalog](/documentation/transforms/python/overview/).
