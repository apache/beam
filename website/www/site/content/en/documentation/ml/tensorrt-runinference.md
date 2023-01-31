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

# Using TensorRT with RunInference
- [NVIDIA TensorRT](https://developer.nvidia.com/tensorrt) is an SDK that facilitates high-performance machine learning inference. It is designed to work with deep learning frameworks such as TensorFlow, PyTorch, and MXNet. It focuses specifically on optimizing and running a trained neural network for inference efficiently on NVIDIA GPUs. TensorRT can maximize inference throughput with multiple optimizations while preserving model accuracy including model quantization, layer and tensor fusions, kernel auto-tuning, multi-stream executions, and efficient tensor memory usage.

- In Apache Beam 2.43.0, Beam introduced the `TensorRTEngineHandler`, which lets you deploy a TensorRT engine in a Beam pipeline. The RunInference transform simplifies the ML pipeline creation process by allowing developers to use Sklearn, PyTorch, TensorFlow and now TensorRT models in production pipelines without needing lots of boilerplate code.

Below, you can find an example that demonstrates how to utilize TensorRT with the RunInference API using a BERT-based text classification model in a Beam pipeline.

# Build a TensorRT engine for inference
To use TensorRT with Apache Beam, you need a converted TensorRT engine file from a trained model. We take a trained BERT based text classification model that does sentiment analysis, i.e. classifies any text into two classes: positive or negative. The trained model is easily available [here](https://huggingface.co/textattack/bert-base-uncased-SST-2). In order to convert the PyTorch Model to TensorRT engine, you need to first convert the model to ONNX and then from ONNX to TensorRT.

### Conversion to ONNX

Using HuggingFace `transformers` libray, one can easily convert a PyTorch model to ONNX. A detailed blogpost can be found [here](https://huggingface.co/blog/convert-transformers-to-onnx) which also mentions the required packages to install. The code that we used for the conversion can be found below.

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

In order to convert an ONNX model to a TensorRT engine you can use the following command from `CLI`:
```
trtexec --onnx=<path to onnx model> --saveEngine=<path to save TensorRT engine> --useCudaGraph --verbose
```

For using `trtexec`, you can follow this [blogpost](https://developer.nvidia.com/blog/simplifying-and-accelerating-machine-learning-predictions-in-apache-beam-with-nvidia-tensorrt/) which builds a docker image built from a DockerFile. The dockerFile that we used is similar to it and can be found below:

```
ARG BUILD_IMAGE=nvcr.io/nvidia/tensorrt:22.05-py3

FROM ${BUILD_IMAGE}

ENV PATH="/usr/src/tensorrt/bin:${PATH}"

WORKDIR /workspace

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository universe && \
    apt-get update && \
    apt-get install -y python3.8-venv

RUN pip install --no-cache-dir apache-beam[gcp]==2.43.0
COPY --from=apache/beam_python3.8_sdk:2.43.0 /opt/apache/beam /opt/apache/beam

RUN pip install --upgrade pip \
    && pip install torch==1.13.1 \
    && pip install torchvision>=0.8.2 \
    && pip install pillow>=8.0.0 \
    && pip install transformers>=4.18.0 \
    && pip install cuda-python

ENTRYPOINT [ "/opt/apache/beam/boot" ]
```
The blogpost also contains the instructions on how to test the TensorRT engine locally.


## Running TensorRT Engine with RunInference in a Beam Pipeline

Now that you have the TensorRT engine, you can use TensorRT engine with RunInferece in a Beam pipeline that can be run both locally and on GCP.

The following code example is a part of the pipeline, where you use `TensorRTEngineHandlerNumPy` to load the TensorRT engine and set other inference parameters.

```
  model_handler = TensorRTEngineHandlerNumPy(
      min_batch_size=1,
      max_batch_size=1,
      engine_path=known_args.trt_model_path,
  )

  task_sentences = [
      "Hello, my dog is cute",
      "I hate you",
      "Shubham Krishna is a good coder",
  ] * 4000

  tokenizer = AutoTokenizer.from_pretrained(known_args.model_id)

  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | "CreateInputs" >> beam.Create(task_sentences)
        | "Preprocess" >> beam.ParDo(Preprocess(tokenizer=tokenizer))
        | "RunInference" >> RunInference(model_handler=model_handler)
        | "PostProcess" >> beam.ParDo(Postprocess(tokenizer=tokenizer)))
```

The full code can be found [here]().

To run this job on Dataflow, run the following command locally:

```
python tensorrt_text_classification.py \
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
--sdk_container_image="us.gcr.io/{GCP_PROJECT}/{MY_DIR}/tensor_rt tensor_rt"
```



## Dataflow Benchmarking