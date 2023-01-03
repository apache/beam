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

The pipeline is written in Java and reads the input data from Google Cloud Storage. With the help of a [PythonExternalTransform](https://beam.apache.org/documentation/programming-guide/#1312-creating-cross-language-python-transforms),
a composite Python transform is called to do the preprocessing, postprocessing, and inference.
Lastly, the data is written back to Google Cloud Storage in the Java pipeline.

## NLP model and dataset
A `bert-base-uncased` natural language processing (NLP) model is used to make inference. This model is open source and available on [HuggingFace](https://huggingface.co/bert-base-uncased). This BERT-model is
used to predict the last word of a sentence based on the context of the sentence.

We also use an [IMDB movie reviews](https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews?select=IMDB+Dataset.csv) dataset, which is  an open-source dataset that is available on Kaggle.

The following is a sample of the data after preprocessing:

| **Text** 	|   **Last Word** 	|
|---	|:---	|
|<img width=700/>|<img width=100/>|
| One of the other reviewers has mentioned that after watching just 1 Oz episode you'll be [MASK] 	| hooked 	|
| A wonderful little [MASK] 	| production 	|
| So im not a big fan of Boll's work but then again not many [MASK] 	| are 	|
| This a fantastic movie of three prisoners who become [MASK] 	| famous 	|
| Some films just simply should not be [MASK] 	| remade 	|
| The Karen Carpenter Story shows a little more about singer Karen Carpenter's complex [MASK] 	| life 	|

You can see the full code used in this example on [Github](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/multi_language_inference).


## Multi-language Inference pipeline

When using multi-language pipelines, you have access to a much larger pool of transforms. For more information, see [Multi-language pipelines](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines) in the Apache Beam Programming Guide.

### Cross-Language Python transform
In addition to running inference, we also need to perform preprocessing and postprocessing on the data. Processing the data makes it possible to interpret the output. In order to do these three tasks, one single composite custom PTransform is written, with a unit DoFn or PTransform for each of the tasks, shown in the following example:

```python
def expand(self, pcoll):
    return (
    pcoll
    | 'Preprocess' >> beam.ParDo(self.Preprocess(self._tokenizer))
    | 'Inference' >> RunInference(KeyedModelHandler(self._model_handler))
    | 'Postprocess' >> beam.ParDo(self.Postprocess(
        self._tokenizer))
    )
```

First, the preprocessing of the data. In this case, the raw textual data is cleaned and tokenized for the BERT-model. All these steps are run in the `Preprocess` DoFn. The `Preprocess` DoFn takes a single element as input and returns a list with both the original text and the tokenized text.

The preprocessed data is then used to make inference. This is done in the [`RunInference`](https://beam.apache.org/documentation/ml/overview/#runinference) PTransform, which is already available in the Apache Beam SDK. The `RunInference` PTransform requires one parameter, a model handler. In this example the `KeyedModelHandler` is used, because the `Preprocess` DoFn also outputs the original sentence. You can change how preprocessing is done based on your requirements. This model handler is defined in the following initialization function of the composite PTransform:

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
The `PytorchModelHandlerKeyedTensorWrapper`, a wrapper around the `PytorchModelHandlerKeyedTensor` model handler, is used. The `PytorchModelHandlerKeyedTensor` model handler makes inference on a PyTorch model. Because the tokenized strings generated from `BertTokenizer` might have different lengths and stack() requires tensors to be the same size, the `PytorchModelHandlerKeyedTensorWrapper` limits the batch size to 1. Restricting `max_batch_size` to 1 means the run_inference() call contains one example per batch. The following example shows the definition of the wrapper:

```python
class PytorchModelHandlerKeyedTensorWrapper(PytorchModelHandlerKeyedTensor):

    def batch_elements_kwargs(self):
      return {'max_batch_size': 1}
```
An alternative aproach is to make all the tensors have the same length. This [example](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch_tensorflow_sklearn.ipynb) shows how to do that.


The `ModelConfig` and `ModelTokenizer` are loaded in the initialization function. The `ModelConfig` is used to define the model architecture, and the `ModelTokenizer` is used to tokenize the input data. The following two parameters are used for these tasks:
- `model`: The name of the model that is used for inference. In this example it is `bert-base-uncased`.
- `model_path`: The path to the `state_dict` of the model that is used for inference. In this example it is a path to a Google Cloud Storage bucket, where the `state_dict` is stored.

Both of these parameters are specified in the Java `PipelineOptions`.

Finally, postprocess the model predictions in the `Postprocess` DoFn. The `Postprocess` DoFn returns the original text, the last word of the sentence, and the predicted word.

### Set up the expansion service
***Note**: In Apache Beam 2.44.0 and later versions, you can use the `withExtraPackages` method to specify the required local packages directly in the Java pipeline. This means you do not need to set up an expansion service anymore and can skip this step.*

Because this example uses transforms from two different languages, we need an SDK for each language, in this case Python and Java. We also need to set up an expansion service, which is used to inject the cross-language Python transform into the Java pipeline. This should be done in a virtual environment which is setup for Beam. [This](https://beam.apache.org/get-started/quickstart-py/#create-and-activate-a-virtual-environment) guide shows how set it up.

To set up the expansion service in the created virtual environment, run the following command in the terminal:

```bash
export PORT = <port to host expansion service>
export IMAGE = <custom docker image>

python -m multi_language_custom_transform.start_expansion_service  \
    --port=$PORT \
    --fully_qualified_name_glob="*" \
    --environment_config=$IMAGE \
    --environment_type=DOCKER
```

This runs the `run_inference_expansion.py` script, which starts the expansion service. Specify two important parameters. First, the `port` parameter specifies the port that the expansion service is hosted on. Second, the `environment_config` parameter specifies the docker image used to run the Python transform. The docker image must contain the Apache Beam Python SDK and all of the other dependencies required to run the transform. For this example, the composite Python PTransform, from the previous section, is wrapped in a local package. Next, we install this local package on the custom docker image.

If no custom transforms are used, you can use the default Apache Beam image. This image is used automatically if you don't specify an `environment_config` parameter. In this scenario, you also don't need to start up an explicit expansion service. The expansion service is started automatically when `PythonExternalTransform` is called.

### Run the Java pipeline
The Java pipeline is defined in the `MultiLangRunInference` class. In this pipeline, the data is read from Google Cloud Storage, the cross-language Python transform is applied, and the output is written to Google Cloud Storage. The following example shows the pipeline:

```java
Pipeline p = Pipeline.create(options);
    PCollection<String> input = p.apply("Read Input", TextIO.read().from(options.getInputFile()));

    input.apply("Predict", PythonExternalTransform.
        <PCollection<String>PCollection<String>>from(
            "multi_language_custom_transform.\
                composite_transform.InferenceTransform", "localhost:" + options.getPort())
            .withKwarg("model",  options.getModelName())
            .withKwarg("model_path", options.getModelPath()))
            .apply("Write Output", TextIO.write().to(options.getOutputFile()
        )
    );
    p.run().waitUntilFinish();
```

Use `PythonExternalTransform` to inject the cross-language Python transform. `PythonExternalTransform` takes two parameters. The first parameter is the fully qualified name of the Python transform. The second parameter is the expansion service address. The `withKwarg` method is used to specify the parameters that are needed for the Python transform. In this example the `model` and `model_path` parameters are specified. These parameters are used in the initialization function of the composite Python PTransform, as shown in the first section.

To run the pipeline, use the following command:

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
The standard Google Cloud and Runner parameters are specified. The `inputFile` and `outputFile` parameters are used to specify the input and output files. The `modelPath` and `modelName` custom parameters are passed to the `PythonExternalTransform`. The port parameter is used to specify the port on which the expansion service is hosted.

## Final remarks
Use this example as a base to create other custom multi-language transforms. You can also use other SDKs. For example, Go also has a wrapper that can make cross-language transforms. For more information, see [Using cross-language transforms in a Go pipeline](https://beam.apache.org/documentation/programming-guide/#1323-using-cross-language-transforms-in-a-go-pipeline) in the Apache Beam Programming Guide.

The full code used in this example can be found on [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/multi_language_inference).
