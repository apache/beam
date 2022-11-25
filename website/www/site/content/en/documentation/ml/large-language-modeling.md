---
title: "Large Language Model Inference in Beam"
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
Staring from Apache Beam 2.40.0, Beam introduced the RunInference API that lets you deploy a machine learning model in a Beam pipeline. A `RunInference` transform performs inference on a `PCollection` of examples using a machine learning (ML) model. The transform outputs a PCollection that contains the input examples and output predictions. You can find more information about RunInference [here](https://beam.apache.org/documentation/transforms/python/elementwise/runinference/) and some examples [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference).


## Using RunInference with very large models
RunInference doesn't only help you deploying small sized models but also any kind of large scale models.

We will demonstrate doing inference with  `T5` language model using `RunInference` in a pipeline. `T5` is an encoder-decoder model pre-trained on a multi-task mixture of unsupervised and supervised tasks and for which each task is converted into a text-to-text format. We will be using `T5-11B` which contains `11 Billion` parameters and is `45 GB` in size. `T5` works well on a variety of tasks out-of-the-box by prepending a different prefix to the input corresponding to each task, e.g., for translation: translate English to German: …, for summarization: summarize: …. You can find more information about the `T5` [here](https://huggingface.co/docs/transformers/model_doc/t5).

### How to Run the Pipeline ?
First, make sure you have installed the required packages and pass the required arguments.
You can find the code [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/large_language_modeling/main.py)

1. Locally on your machine: `python main.py --runner DirectRunner`
2. On GCP using Dataflow: `python main.py --runner DataflowRunner`

### Explaining the Pipeline
The pipeline can be broken down into few simple steps:
1. Reading the inputs
2. Encoding the text into transformer-readable token ID integers using a tokenizer
3. Using RunInference to get the output
4. Decoding the RunInference output and printing them

The code snippet for all the 4 steps can be found below:

{{< /highlight >}}

    with beam.Pipeline(options=pipeline_options) as pipeline:
        _ = (
            pipeline
            | "CreateInputs" >> beam.Create(task_sentences)
            | "Preprocess" >> beam.ParDo(Preprocess(tokenizer=tokenizer))
            | "RunInference" >> RunInference(model_handler=model_handler)
            | "PostProcess" >> beam.ParDo(Postprocess(tokenizer=tokenizer))
        )

{{< /highlight >}}

We now closely look at the 3rd step of pipeline where we use `RunInference`.
In order to use it, one has to first define a `ModelHandler`. RunInference provides model handlers for `PyTorch`, `TensorFlow` and `Scikit-Learn`. As, we are using a `PyTorch` model, so we used a `PyTorchModelHandlerTensor`.

{{< /highlight >}}

  gen_fn = make_tensor_model_fn('generate')

  model_handler = PytorchModelHandlerTensor(
      state_dict_path=args.model_state_dict_path,
      model_class=T5ForConditionalGeneration,
      model_params={"config": AutoConfig.from_pretrained(args.model_name)},
      device="cpu",
      inference_fn=gen_fn)

{{< /highlight >}}

`ModelHandler` requires parameters like:
* `state_dict_path` – path to the saved dictionary of the model state.
* `model_class` – class of the Pytorch model that defines the model structure.
* `model_params` – A dictionary of arguments required to instantiate the model class.
* `device` – the device on which you wish to run the model. If device = GPU then a GPU device will be used if it is available. Otherwise, it will be CPU.
* `inference_fn` -  the inference function to use during RunInference.
