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

# Using RunInference from Java SDK

The pipeline in this example is written in Java and reads the input data from Google Cloud Storage. With the help of a [PythonExternalTransform](https://beam.apache.org/documentation/programming-guide/#1312-creating-cross-language-python-transforms),
a composite Python transform is called to do the preprocessing, postprocessing, and inference.
Lastly, the data is written back to Google Cloud Storage in the Java pipeline.

You can find the code used in this example in the [Beam repository](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/multi_language_inference).
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


## Multi-language Inference pipeline

When using multi-language pipelines, you have access to a much larger pool of transforms. For more information, see [Multi-language pipelines](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines) in the Apache Beam Programming Guide.

### Custom Python transform
In addition to running inference, we also need to perform preprocessing and postprocessing on the data. Postprocessing the data makes it possible to interpret the output. In order to do these three tasks, one single composite custom PTransform is written, with a unit DoFn or PTransform for each of the tasks, as shown in the following snippet:

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
The `PytorchModelHandlerKeyedTensorWrapper`, a wrapper around the `PytorchModelHandlerKeyedTensor` model handler, is used. The `PytorchModelHandlerKeyedTensor` model handler makes inference on a PyTorch model. Because the tokenized strings generated from `BertTokenizer` might have different lengths and stack() requires tensors to be the same size, the `PytorchModelHandlerKeyedTensorWrapper` limits the batch size to 1. Restricting `max_batch_size` to 1 means the run_inference() call contains one example per batch. The following code shows the definition of the wrapper:

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

Finally, we postprocess the model predictions in the `Postprocess` DoFn. The `Postprocess` DoFn returns the original text, the last word of the sentence, and the predicted word.

### Compile Python code into package

The custom Python code needs to be written in a local package and be compiled as a tarball. This package can then be used by the Java pipeline. The following example shows how to compile the Python package into a tarball:

```bash
 python setup.py sdist
 ```

In order to run this, a `setup.py` is required. The path to the tarball will be used as an argument in the pipeline options of the Java pipeline.
### Run the Java pipeline
The Java pipeline is defined in the [`MultiLangRunInference`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/multi_language_inference/last_word_prediction/src/main/java/org/apache/beam/examples/MultiLangRunInference.java#L32) class. In this pipeline, the data is read from Google Cloud Storage, the cross-language Python transform is applied, and the output is written back to Google Cloud Storage.

The `PythonExternalTransform` is used to inject the cross-language Python transform into the Java pipeline. `PythonExternalTransform` takes a string parameter which is the fully qualified name of the Python transform.

The `withKwarg` method is used to specify the parameters that are needed for the Python transform. In this example the `model` and `model_path` parameters are specified. These parameters are used in the initialization function of the composite Python PTransform, as shown in the first section. Finally the `withExtraPackages` method is used to specify the additional Python dependencies that are needed for the Python transform. In this example the `local_packages` list is used, which contains Python requirements and the path to the compiled tarball.

To run the pipeline, use the following command:

```bash
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.MultiLangRunInference \
    -Dexec.args="--runner=DataflowRunner \
                 --project=$GCP_PROJECT\
                 --region=$GCP_REGION \
                 --gcpTempLocation=gs://$GCP_BUCKET/temp/ \
                 --inputFile=gs://$GCP_BUCKET/input/imdb_reviews.csv \
                 --outputFile=gs://$GCP_BUCKET/output/ouput.txt \
                 --modelPath=gs://$GCP_BUCKET/input/bert-model/bert-base-uncased.pth \
                 --modelName=$MODEL_NAME \
                 --localPackage=$LOCAL_PACKAGE" \
    -Pdataflow-runner
```
The standard Google Cloud and Runner parameters are specified. The `inputFile` and `outputFile` parameters are used to specify the input and output files. The `modelPath` and `modelName` custom parameters are passed to the `PythonExternalTransform`. Finally the `localPackage` parameter is used to specify the path to the compiled Python package, which contains the custom Python transform.

## Final remarks
Use this example as a base to create other custom multi-language inference pipelines. You can also use other SDKs. For example, Go also has a wrapper that can make cross-language transforms. For more information, see [Using cross-language transforms in a Go pipeline](https://beam.apache.org/documentation/programming-guide/#1323-using-cross-language-transforms-in-a-go-pipeline) in the Apache Beam Programming Guide.

The full code used in this example can be found on [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/multi_language_inference).
