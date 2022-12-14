---
title: "Cross Language RunInference  "
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

# Cross Language RunInference

This Cross Language RunInference example shows how to use the [RunInference](https://beam.apache.org/documentation/ml/overview/#runinference)
Transform in a multi-language pipeline. The pipeline is in Java and reads the input data from
GCS. With the help of a [PythonExternalTransform](https://beam.apache.org/documentation/programming-guide/#1312-creating-cross-language-python-transforms)
a composite python transform is called that does the preprocessing, postprocessing and inference.
Lastly, the data is written back to GCS in the Java pipeline.

## NLP model and dataset
A `bert-base-uncased` model is used to make inference, which is an open-source model
available on [HuggingFace](https://huggingface.co/bert-base-uncased). This BERT-model will be
used to predict the last word of a sentence, based on the context of the sentence.

Next to this we also use an [IMDB movie reviews](https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews?select=IMDB+Dataset.csv) dataset, which is  an open-source dataset that is available on Kaggle.  A sample of the data after preprocessing is shown below:

| **Text** 	|   **Last Word** 	|
|---	|:---	|
|<img width=700/>|<img width=100/>|
| One of the other reviewers has mentioned that after watching just 1 Oz episode you'll be [MASK] 	| hooked 	|
| A wonderful little [MASK] 	| production 	|
| So im not a big fan of Boll's work but then again not many [MASK] 	| are 	|
| This a fantastic movie of three prisoners who become [MASK] 	| famous 	|
| Some films just simply should not be [MASK] 	| remade 	|
| The Karen Carpenter Story shows a little more about singer Karen Carpenter's complex [MASK] 	| life 	|

The full code used in this example can be found on GitHub [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/multi_language_inference).


## Multi-language RunInference pipeline
### Cross-Language Python transform
Next to making inference on the data, we also need to perform preprocessing and postprocessing on the data. This way the pipeline gives clean output that is easily interpreted.  In order to do these three tasks, one single composite custom Ptransform is written, with a unit DoFn or PTransform for each of the tasks as shown below:

```python
def expand(self, pcoll):
    return (
    pcoll
    | 'Preprocess' >> beam.ParDo(self.Preprocess(self._tokenizer))
    | 'Inference' >> RunInference(KeyedModelHandler(self._model_handler))
    | 'Postprocess' >> beam.ParDo(self.Postprocess(
        self._tokenizer)).with_input_types(typing.Iterable[str])
    )
```

First, the preprocessing is done. In which the raw textual data is cleaned and tokenized for the BERT-model. All these steps are executed in the `Preprocess` DoFn. The `Preprocess` DoFn takes a single element as input and returns list with the original text and the tokenized text.

The preprocessed data is then used to make inference. This is done in the [`RunInference`](https://beam.apache.org/documentation/ml/overview/#runinference) PTransform, which is already available in the Apache Beam SDK. The `RunInference` PTransform requires one parameter, a modelhandler. In this example the `KeyedModelHandler` is used, because the `Preprocess` Dofn also output the original sentence. Ofcourse, this is personal preference and can be changed to the needs of the end-user. This modelhandler is defined it this initialization function of the composite Ptransform. This section is shown below:

```python
def __init__(self, model, model_path):
    self._model = model
    logging.info(f"Downloading {self._model} model from GCS.")
    self._model_config = BertConfig.from_pretrained(self._model)
    self._tokenizer = BertTokenizer.from_pretrained(self._model)
    self._model_handler = self.PytorchModelHandlerKeyedTensorWrapper(
        state_dict_path=(model_path),
        model_class=BertForMaskedLM,
        model_params={'config': self._model_config},
        device='cuda:0')
```
We can see that the `PytorchModelHandlerKeyedTensorWrapper` is used. This is a wrapper around the `PytorchModelHandlerKeyedTensor` modelhandler. The `PytorchModelHandlerKeyedTensor` modelhandler is used to make inference on a PyTorch model. The `PytorchModelHandlerKeyedTensorWrapper` is used to limit the batch size to 1. This is done because the tokenized strings generated from BertTokenizer may have different lengths, which doesn't work with torch.stack() in current RunInference implementation since stack() requires tensors to be the same size. Restricting max_batch_size to 1 means there is only 1 example per batch in the run_inference() call. The definition of the wrapper is shown below:

```python
class PytorchModelHandlerKeyedTensorWrapper(PytorchModelHandlerKeyedTensor):

    def batch_elements_kwargs(self):
      return {'max_batch_size': 1}
```

Next to the definition of the modelhandler, the ModelConfig and ModelTokenizer are loaded in the initialization function. The ModelConfig is used to define the model architecture and the ModelTokenizer is used to tokenize the input data. This is done with the following two parameters:
- `model`: The name of the model that is used for inference. In this example it is `bert-base-uncased`.
- `model_path`: The path to the state_dict of the model that is used for inference. In this example it is a path to a GCS bucket, where the state_dict is stored.

Both these parameters specified in the Java PipelineOptions.

Finally the predictions of the model are postprocessed. This is done in the `Postprocess` DoFn. The `Postprocess` DoFn returns the original text, the last word of the sentence and the predicted word.

### Set up the expansion service
Because we are using transforms from two different languages, we need an SDK for each language (in this case Python and Java). Next to this we also need to set up an expansion service. More specifically, the expansion service is used to inject the cross-language Python transform into the Java pipeline. By opting for multi-language pipelines, you have access to a much bigger pool of transforms. More detailed information can be found [here](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines).


Setting up the expansion service is pretty trivial. We just need to run the following command in the terminal:

```bash
export PORT = <port to host expansion service>
export IMAGE = <custom docker image>

python -m expansion_service.start_expansion_service  \
    --port=$PORT \
    --fully_qualified_name_glob="*" \
    --environment_config=$IMAGE \
    --environment_type=DOCKER
```


This runs the `run_inference_expansion.py` script which starts up the expansion service. Two important parameters should be specified. First of all the `port` parameter is used to specify the port on which the expansion service will be hosted. Secondly, the `environment_config` parameter is used to specify the docker image that will be used to execute the Python transform. The docker image should contain the Python SDK and all the dependencies needed to run the transform. So for this example the composite Python PTransform, from the previous section, is wrapped in a local package. Next we install this  local package on the custom Docker image.

If no custom transforms are used, the default apache beam image can be used. This is done automatically if no environment_config parameter is specified. In addition to this, there is also no need to start up an explicit expansion service. The expansion service will be started automatically when the `PythonExternalTransform` is called.

**Note**: from 2.44.0 on you can specifiy the required local packages directly in the Java Pipeline with the `withExtraPackages` method. This way, the default apache beam image can be used again, which simplifies the setup of the expansion service.

### Run the Java pipeline
The Java pipeline is pretty straightforward. The pipeline is defined in the `MultiLangRunInference` class. In this pipeline, the data is read from GCS, the cross-language Python transform is applied and the output is written to GCS. The pipeline is shown below:

```java
Pipeline p = Pipeline.create(options);
    PCollection<String> input = p.apply("Read Input", TextIO.read().from(options.getInputFile()));

    input.apply("Predict", PythonExternalTransform.
        <PCollection<String>PCollection<String>>from(
            "expansion_service.run_inference_expansion.\
            RunInferenceTransform", "localhost:" + options.getPort())
            .withKwarg("model",  options.getModelName())
            .withKwarg("model_path", options.getModelPath()))
            .apply("Write Output", TextIO.write().to(options.getOutputFile()
        )
    );
    p.run().waitUntilFinish();
```

As previously mentioned the `PythonExternalTransform` is used to inject the cross-language Python transform. The `PythonExternalTransform` takes two parameters. The first parameter is the fully qualified name of the Python transform. The second parameter is the expansion service address. The `withKwarg` method is used to specify the parameters that are needed for the Python transform. In this example the `model` and `model_path` parameters are specified. These parameters are used in the initialization function of the composite Python PTransform, as shown in the first section.

In order to run the pipeline, the following command can be used:

```bash
mvn compile exec:java -Dexec.mainClass=org.MultiLangRunInference \
    -Dexec.args="--runner=DataflowRunner --project=$GCP_PROJECT\
                 --region=$GCP_REGION \
                 --gcpTempLocation=gs://$GCP_BUCKET/temp/ \
                 --inputFile=gs://$GCP_BUCKET/input/imdb_reviews.csv \
                 --outputFile=gs://$GCP_BUCKET/output/ouput.txt \
                 --modelPath=gs://$GCP_BUCKET/input/bert-model/bert-base-uncased.pth \
                 --modelName=$MODEL_NAME \
                 --port=$PORT" \
    -Pdataflow-runner \
    -e
```
The standard GCP and Runner parameters are specified. The `inputFile` and `outputFile` parameters are used to specify the input and output files. The `modelPath` and `modelName` custom parameters which are passed to the `PythonExternalTransform`.  The `port` parameter is used to specify the port on which the expansion service is hosted.

## Final remarks
This example should serve as a base to create other custom multi-language transforms. Other SDK's are also an option for multi-language. For example, GO also has a wrapper which can can cross-language transforms. More information can be found [here](https://beam.apache.org/documentation/programming-guide/#1323-using-cross-language-transforms-in-a-go-pipeline).

The full code used in this example can be found on GitHub [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/multi_language_inference).
