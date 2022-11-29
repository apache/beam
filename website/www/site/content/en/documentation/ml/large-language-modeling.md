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
In Apache Beam 2.40.0, Beam introduced the RunInference API, which lets you deploy a machine learning model in a Beam pipeline. A `RunInference` transform performs inference on a `PCollection` of examples using a machine learning (ML) model. The transform outputs a PCollection that contains the input examples and output predictions. For more information, see RunInference [here](https://beam.apache.org/documentation/transforms/python/elementwise/runinference/). You can also find [inference examples on GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference).


## Using RunInference with very large models
RunInference works well on arbitrarily large models as long as they can fit on your hardware.

This example demonstrates running inference with a `T5` language model using `RunInference` in a pipeline. `T5` is an encoder-decoder model pre-trained on a multi-task mixture of unsupervised and supervised tasks. Each task is converted into a text-to-text format. The example uses `T5-11B`, which contains 11 billion parameters and is 45 GB in size. In  order to work well on a variety of tasks, `T5` prepends a different prefix to the input corresponding to each task. For example, for translation, the input would be: `translate English to German: …` and for summarization, it would be: `summarize: …`. For more information about `T5` see the [T5 overiew](https://huggingface.co/docs/transformers/model_doc/t5) in the HuggingFace documentation.

### Run the Pipeline ?
First, install the required packages and pass the required arguments.
You can view the code on [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/large_language_modeling/main.py)

1. Locally on your machine: `python main.py --runner DirectRunner`. You need to have 45 GB of disk space available to run this example.
2. On Google Cloud using Dataflow: `python main.py --runner DataflowRunner`

### Pipeline Steps
The pipeline contains the following steps:
1. Read the inputs.
2. Encode the text into transformer-readable token ID integers using a tokenizer.
3. Use RunInference to get the output.
4. Decode the RunInference output and print it.

The following code snippet contains the four steps:

{{< highlight >}}
    with beam.Pipeline(options=pipeline_options) as pipeline:
        _ = (
            pipeline
            | "CreateInputs" >> beam.Create(task_sentences)
            | "Preprocess" >> beam.ParDo(Preprocess(tokenizer=tokenizer))
            | "RunInference" >> RunInference(model_handler=model_handler)
            | "PostProcess" >> beam.ParDo(Postprocess(tokenizer=tokenizer))
        )
{{< /highlight >}}

In the third step of pipeline we use `RunInference`.
In order to use it, you must first define a `ModelHandler`. RunInference provides model handlers for `PyTorch`, `TensorFlow` and `Scikit-Learn`. Because the example uses a `PyTorch` model, it uses the `PyTorchModelHandlerTensor` model handler.

{{< highlight >}}
  gen_fn = make_tensor_model_fn('generate')

  model_handler = PytorchModelHandlerTensor(
      state_dict_path=args.model_state_dict_path,
      model_class=T5ForConditionalGeneration,
      model_params={"config": AutoConfig.from_pretrained(args.model_name)},
      device="cpu",
      inference_fn=gen_fn)
{{< /highlight >}}

A `ModelHandler` requires parameters like:
* `state_dict_path` – The path to the saved dictionary of the model state.
* `model_class` – The class of the Pytorch model that defines the model structure.
* `model_params` – A dictionary of arguments required to instantiate the model class.
* `device` – The device on which you wish to run the model. If device = GPU then a GPU device will be used if it is available. Otherwise, it will be CPU.
* `inference_fn` -  The inference function to use during RunInference.
