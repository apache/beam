# RunInference Benchmarks

This module contains benchmarks used to test the performance of the RunInference transform
running inference with common models and frameworks. Each benchmark is explained in detail
below. Beam's performance over time can be viewed at http://s.apache.org/beam-community-metrics/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1

## Pytorch RunInference Image Classification 50K

The Pytorch RunInference Image Classification 50K benchmark runs an
[example image classification pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_image_classification.py)
using various different resnet image classification models (the benchmarks on
[Beam's dashboard](http://s.apache.org/beam-community-metrics/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1)
display [resnet101](https://huggingface.co/microsoft/resnet-101) and [resnet152](https://huggingface.co/microsoft/resnet-152))
against 50,000 example images from the OpenImage dataset. The benchmarks produce
the following metrics:

- Mean Inference Requested Batch Size - the average batch size that RunInference groups the images into for batch prediction
- Mean Inference Batch Latency - the average amount of time it takes to perform inference on a given batch of images
- Mean Load Model Latency - the average amount of time it takes to load a model. This is done once per DoFn instance on worker
startup, so the cost is amortized across the pipeline.

## Pytorch RunInference Language Modeling

The Pytorch RunInference Language Modeling benchmark runs an
[example language modeling pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_language_modeling.py)
using the [Bert large uncased](https://huggingface.co/bert-large-uncased)
and [Bert base uncased](https://huggingface.co/bert-base-uncased) models
and a dataset of 50,000 manually generated sentences. The benchmarks produce
the following metrics:

- Mean Inference Requested Batch Size - the average batch size that RunInference groups the images into for batch prediction
- Mean Inference Batch Latency - the average amount of time it takes to perform inference on a given batch of images
- Mean Load Model Latency - the average amount of time it takes to load a model. This is done once per DoFn instance on worker
startup, so the cost is amortized across the pipeline.