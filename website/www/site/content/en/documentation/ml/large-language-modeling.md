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

# Large Language Model Inference in Beam
In Apache Beam 2.40.0, Beam introduced the RunInference API, which lets you deploy a machine learning model in a Beam pipeline. A `RunInference` transform performs inference on a `PCollection` of examples using a machine learning (ML) model. The transform outputs a PCollection that contains the input examples and output predictions. For more information, see RunInference [here](/documentation/transforms/python/elementwise/runinference/). You can also find [inference examples on GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference).

## Using RunInference with very large models
RunInference works well on arbitrarily large models as long as they can fit on your hardware.

### Memory Management

RunInference has several mechanisms for reducing memory utilization. For example, by default RunInference load at most a single copy of each model per process (rather than one per thread).

Many Beam runners, however, run multiple Beam processes per machine at once. This can cause problems since the memory footprint of loading large models like LLMs multiple times can be too large to fit into a single machine.
For memory-intensive models, RunInference provides a mechanism for more intelligently sharing memory across multiple processes to reduce the overall memory footprint. To enable this mode, users just have
to set the parameter `large_model` to True in their model configuration (see below for an example), and Beam will take care of the memory management. When using a custom model handler, you can override the `share_model_across_processes` function or the `model_copies` function for a similar effect.

### Running an Example Pipeline with T5

This example demonstrates running inference with a `T5` language model using `RunInference` in a pipeline. `T5` is an encoder-decoder model pre-trained on a multi-task mixture of unsupervised and supervised tasks. Each task is converted into a text-to-text format. The example uses `T5-11B`, which contains 11 billion parameters and is 45 GB in size. In  order to work well on a variety of tasks, `T5` prepends a different prefix to the input corresponding to each task. For example, for translation, the input would be: `translate English to German: …` and for summarization, it would be: `summarize: …`. For more information about `T5` see the [T5 overiew](https://huggingface.co/docs/transformers/model_doc/t5) in the HuggingFace documentation.

To run inference with this model, first, install `apache-beam` 2.40 or greater:

```
pip install apache-beam -U
```

Next, install the required packages listed in [requirements.txt](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/large_language_modeling/requirements.txt) and pass the required arguments. You can download the `T5-11b` model from [Hugging Face Hub](https://huggingface.co/t5-11b) with the following steps:

- Install Git LFS following the instructions [here](https://docs.github.com/en/repositories/working-with-files/managing-large-files/installing-git-large-file-storage?platform=mac)
- Run `git lfs install`
- Run `git clone https://huggingface.co/t5-11b` (this may take a long time). This will download the checkpoint, then you need to convert it to the model state dict as described [here](https://pytorch.org/tutorials/beginner/saving_loading_models.html#save-load-state-dict-recommended):

```
import torch
from transformers import T5ForConditionalGeneration

model = T5ForConditionalGeneration.from_pretrained("path/to/cloned/t5-11b")
torch.save(model.state_dict(), "path/to/save/state_dict.pth")
```

You can view the code on [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/large_language_modeling/main.py)

1. Locally on your machine:
```
python main.py --runner DirectRunner \
               --model_state_dict_path <local or remote path to state_dict> \
               --model_name t5-11b
```
You need to have 45 GB of disk space available to run this example.

2. On Google Cloud using Dataflow:
```
python main.py --runner DataflowRunner \
                --model_state_dict_path <gs://path/to/saved/state_dict.pth> \
                --model_name t5-11b \
                --project <PROJECT_ID> \
                --region <REGION> \
                --requirements_file requirements.txt \
                --staging_location <gs://path/to/staging/location>
                --temp_location <gs://path/to/temp/location> \
                --experiments "use_runner_v2,no_use_multiple_sdk_containers" \
                --machine_type=n1-highmem-16 \
                --disk_size_gb=200
```

You can also pass other configuration parameters as described [here](https://cloud.google.com/dataflow/docs/guides/setting-pipeline-options#setting_required_options).

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
      inference_fn=gen_fn,
      large_model=True)
{{< /highlight >}}

A `ModelHandler` requires parameters like:
* `state_dict_path` – The path to the saved dictionary of the model state.
* `model_class` – The class of the Pytorch model that defines the model structure.
* `model_params` – A dictionary of arguments required to instantiate the model class.
* `device` – The device on which you wish to run the model. If device = GPU then a GPU device will be used if it is available. Otherwise, it will be CPU.
* `inference_fn` -  The inference function to use during RunInference.
* `large_model` - (see `Memory Management` above). Whether to use memory minimization techniques to lower the memory footprint of your model.

### Troubleshooting Large Models

#### Pickling errors

When sharing a model across processes with `large_model=True` or using a custom model handler, Beam sends the input and output data across a process boundary.
To do this, it uses a serialization method known as [pickling](https://docs.python.org/3/library/pickle.html).
For example, if you call `output=model.my_inference_fn(input_1, input_2)`, `input_1`, `input_2`, and `output` will all need to be pickled.
The model itself does not need to be pickled since it is not passed across process boundaries.

While most objects can be pickled without issue, if one of these objects is unpickleable you may run into errors like `error: can't pickle fasttext_pybind.fasttext objects`.
To work around this, there are a few options:

First of all, if possible you can choose not to share your model across processes. This will incur additional memory pressure, but it may be tolerable in some cases.

Second, using a custom model handler you can wrap your model to take in and return serializable types. For example, if your model handler looks like:

```
class MyModelHandler():
   def load_model(self):
      return model_loading_logic()

   def run_inference(self, batch: Sequence[str], model, inference_args):
      unpickleable_object = Unpickleable(batch)
      unpickleable_returned = model.predict(unpickleable_object)
      my_output = int(unpickleable_returned[0])
      return my_output
```

you could instead wrap the unpickleable pieces in a model wrapper. Since the model wrapper will sit in the inference process, this will work as long as it only takes in/returns pickleable objects.

```
class MyWrapper():
   def __init__(self, model):
      self._model = model

   def predict(self, batch: Sequence[str]):
      unpickleable_object = Unpickleable(batch)
      unpickleable_returned = model.predict(unpickleable_object)
      return int(prediction[0])

class MyModelHandler():
   def load_model(self):
      return MyWrapper(model_loading_logic())

   def run_inference(self, batch: Sequence[str], model: MyWrapper, inference_args):
      return model.predict(unpickleable_object)
```

## RAG and Prompt Engineering in Beam

Beam is also an excellent tool for improving the quality of your LLM prompts using Retrieval Augmented Generation (RAG).
Retrieval augmented generation is a technique that enhances large language models (LLMs) by connecting them to external knowledge sources.
This allows the LLM to access and process real-time information, improving the accuracy, relevance, and factuality of its responses.

Beam has several mechanisms to make this process simpler:

1. Beam's [MLTransform](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.transforms.embeddings.html) provides an embeddings package to generate the embeddings used for RAG. You can also use RunInference to generate embeddings if you have a model without an embeddings handler.
2. Beam's [Enrichment transform](https://beam.apache.org/documentation/transforms/python/elementwise/enrichment/) makes it easy to look up embeddings or other information in an external storage system like a [vector database](https://www.pinecone.io/learn/vector-database/).

Collectively, you can use these to perform RAG using the following steps:

**Pipeline 1 - generate knowledge base:**

1. Ingest data from external source using one of [Beam's IO connectors](https://beam.apache.org/documentation/io/connectors/)
2. Generate embeddings on that data using [MLTransform](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.transforms.embeddings.html)
3. Write those embeddings to a vector DB using a [ParDo](https://beam.apache.org/documentation/programming-guide/#pardo)

**Pipeline 2 - use knowledge base to perform RAG:**

1. Ingest data from external source using one of [Beam's IO connectors](https://beam.apache.org/documentation/io/connectors/)
2. Generate embeddings on that data using [MLTransform](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.transforms.embeddings.html)
3. Enrich that data with additional embeddings from your vector DB using [Enrichment](https://beam.apache.org/documentation/transforms/python/elementwise/enrichment/)
4. Use that enriched data to prompt your LLM with [RunInference](https://beam.apache.org/documentation/transforms/python/elementwise/runinference/)
5. Write that data to your desired sink using one of [Beam's IO connectors](https://beam.apache.org/documentation/io/connectors/)

To view an example pipeline performing RAG, see https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/rag_usecase/beam_rag_notebook.ipynb
