---
title: "TensorRT RunInference"
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

# Use TensorRT with RunInference
- [NVIDIA TensorRT](https://developer.nvidia.com/tensorrt) is an SDK that facilitates high-performance machine learning inference. It is designed to work with deep learning frameworks such as TensorFlow, PyTorch, and MXNet. It focuses specifically on optimizing and running a trained neural network to efficiently run inference on NVIDIA GPUs. TensorRT can maximize inference throughput with multiple optimizations while preserving model accuracy including model quantization, layer and tensor fusions, kernel auto-tuning, multi-stream executions, and efficient tensor memory usage.

- In Apache Beam 2.43.0, Beam introduced the [TensorRTEngineHandler](https://beam.apache.org/releases/pydoc/2.43.0/apache_beam.ml.inference.tensorrt_inference.html#apache_beam.ml.inference.tensorrt_inference.TensorRTEngineHandlerNumPy), which lets you deploy a TensorRT engine in a Beam pipeline. The RunInference transform simplifies the ML inference pipeline creation process by allowing developers to use Sklearn, PyTorch, TensorFlow and now TensorRT models in production pipelines without needing lots of boilerplate code.

The following example that demonstrates how to use TensorRT with the RunInference API using a BERT-based text classification model in a Beam pipeline.

## Build a TensorRT engine for inference
To use TensorRT with Apache Beam, you need a converted TensorRT engine file from a trained model. We take a trained BERT based text classification model that does sentiment analysis and classifies any text into two classes: positive or negative. The trained model is available [from HuggingFace](https://huggingface.co/textattack/bert-base-uncased-SST-2). To convert the PyTorch Model to TensorRT engine, you need to first convert the model to ONNX and then from ONNX to TensorRT.

### Conversion to ONNX

You can use the HuggingFace `transformers` library to convert a PyTorch model to ONNX. For details, see the blog post [Convert Transformers to ONNX with Hugging Face Optimum](https://huggingface.co/blog/convert-transformers-to-onnx). The blog post explains which required packages to install. The following code is used for the conversion.

```
from pathlib import Path
import transformers
from transformers.onnx import FeaturesManager
from transformers import AutoConfig, AutoTokenizer, AutoModelForMaskedLM, AutoModelForSequenceClassification


# load model and tokenizer
model_id = "textattack/bert-base-uncased-SST-2"
feature = "sequence-classification"
model = AutoModelForSequenceClassification.from_pretrained(model_id)
tokenizer = AutoTokenizer.from_pretrained(model_id)

# load config
model_kind, model_onnx_config = FeaturesManager.check_supported_model_or_raise(model, feature=feature)
onnx_config = model_onnx_config(model.config)

# export
onnx_inputs, onnx_outputs = transformers.onnx.export(
        preprocessor=tokenizer,
        model=model,
        config=onnx_config,
        opset=12,
        output=Path("bert-sst2-model.onnx")
)
```

### From ONNX to TensorRT engine

To convert an ONNX model to a TensorRT engine, use the following command from the `CLI`:
```
trtexec --onnx=<path to onnx model> --saveEngine=<path to save TensorRT engine> --useCudaGraph --verbose
```

To use `trtexec`, follow the steps in the blog post [Simplifying and Accelerating Machine Learning Predictions in Apache Beam with NVIDIA TensorRT](https://developer.nvidia.com/blog/simplifying-and-accelerating-machine-learning-predictions-in-apache-beam-with-nvidia-tensorrt/). The post explains how to build a docker image from a DockerFile that can be used for conversion. We use the following Docker file, which is similar to the file used in the blog post:

```
ARG BUILD_IMAGE=nvcr.io/nvidia/tensorrt:22.05-py3

FROM ${BUILD_IMAGE}

ENV PATH="/usr/src/tensorrt/bin:${PATH}"

WORKDIR /workspace

RUN apt-get update -y && apt-get install -y python3-venv
RUN pip install --no-cache-dir apache-beam[gcp]==2.44.0
COPY --from=apache/beam_python3.8_sdk:2.44.0 /opt/apache/beam /opt/apache/beam

RUN pip install --upgrade pip \
    && pip install torch==1.13.1 \
    && pip install torchvision>=0.8.2 \
    && pip install pillow>=8.0.0 \
    && pip install transformers>=4.18.0 \
    && pip install cuda-python

ENTRYPOINT [ "/opt/apache/beam/boot" ]
```
The blog post also contains instructions explaining how to test the TensorRT engine locally.


## Run TensorRT engine with RunInference in a Beam pipeline

Now that you have the TensorRT engine, you can use TensorRT engine with RunInference in a Beam pipeline that can run both locally and on Google Cloud.

The following code example is a part of the pipeline. You use `TensorRTEngineHandlerNumPy` to load the TensorRT engine and to set other inference parameters.

```
  model_handler = TensorRTEngineHandlerNumPy(
      min_batch_size=1,
      max_batch_size=1,
      engine_path=known_args.trt_model_path,
  )

  tokenizer = AutoTokenizer.from_pretrained(known_args.model_id)

  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | "ReadSentences" >> beam.io.ReadFromText(known_args.input)
        | "Preprocess" >> beam.ParDo(Preprocess(tokenizer=tokenizer))
        | "RunInference" >> RunInference(model_handler=model_handler)
        | "PostProcess" >> beam.ParDo(Postprocess(tokenizer=tokenizer)))
```

The full code can be found [on GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/tensorrt_text_classification.py).

To run this job on Dataflow, run the following command locally:

```
python tensorrt_text_classification.py \
--input gs://{GCP_PROJECT}/sentences.txt \
--trt_model_path gs://{GCP_PROJECT}/sst2-text-classification.trt \
--runner DataflowRunner \
--experiment=use_runner_v2 \
--machine_type=n1-standard-4 \
--experiment="worker_accelerator=type:nvidia-tesla-t4;count:1;install-nvidia-driver" \
--disk_size_gb=75 \
--project {GCP_PROJECT} \
--region us-central1 \
--temp_location gs://{GCP_PROJECT}/tmp/ \
--job_name tensorrt-text-classification \
--sdk_container_image="us.gcr.io/{GCP_PROJECT}/{MY_DIR}/tensor_rt"
```



## Dataflow Benchmarking

We ran experiments in Dataflow using a TensorRT engine and the following configurations: `n1-standard-4` machine with a disk size of `75GB`. To mimic data streaming into Dataflow via `PubSub`, we set the batch size to 1 by setting the min and max batch sizes for `ModelHandlers` to 1.

|  | Stage with RunInference | Mean inference_batch_latency_micro_secs|
|:----------:|:----------:|:----------:|
|    TensorFlow with T4 GPU	      | 3 min 1 sec | 15,176 |
| TensorRT with T4 GPU	 | 45 sec | 3,685 |

The Dataflow runner decomposes a pipeline into multiple stages. You can get a better picture of the performance of RunInference by looking at the stage that contains the inference call, and not the other stages that read and write data. This is in the Stage with RunInference column.

The metric `inference_batch_latency_micro_secs` is the time, in microseconds, that it takes to perform the inference on the batch of examples, that is, the time to call `model_handler.run_inference`. This varies over time depending on the dynamic batching decision of BatchElements, and the particular values or dtype values of the elements. For this metric, you can see that TensorRT is about 4.1x faster than TensorFlow.