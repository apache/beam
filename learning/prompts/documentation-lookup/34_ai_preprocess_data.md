Prompt:
How can I preprocess data for AI/ML pipeline with Apache Beam?
Response:
Apache Beam provides a rich set of data processing [transforms](https://beam.apache.org/documentation/programming-guide/#applying-transforms) that can be used to preprocess data for training and inference.
To simplify your data preprocessing, Apache Beam provides a [MLTransform](https://beam.apache.org/documentation/ml/preprocess-data/) that wraps transforms in one class allowing you to ensure data consistency using the same steps for training and inference.

MLTransform can do a full pass on the dataset allowing you to perform the followingcommon preprocessing steps:
- normalization of input values using a minimum-maximum scaling
- convert `floats` to `integers` using a bucketization based on the input data distribution
- convert string to integers by generating a vocabulary over an entire dataset
- calculate [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) weights for text data

For a full list of supported `MLTransforms` and I/O requirements see [here](https://beam.apache.org/documentation/ml/preprocess-data/#data-processing-transforms-that-use-tft) .

MLTransform can operate in read or write mode generating or consuming artifacts such as vocabulary or statistics. See an example of `write mode` below:
```python
  with beam.Pipeline() as p:
    transformed_data = (
        p
        | beam.Create(data)
        | MLTransform(write_artifact_location=artifact_location).with_transform(
            <TRANSFORM_FUNCTION_NAME>)
        | beam.Map(print))

```
For more examples of using `MLTransform` see [here](https://beam.apache.org/documentation/transforms/python/elementwise/mltransform/).

MLTransform also can be used to generate text embeddings to push data into vector stores or to be used as input for machine learning models. Supported text embedding transforms include `SentenceTransformerEmbeddings` for the `Hugging Face` [sentence-transformers](https://huggingface.co/sentence-transformers) and `VertexAITextEmbeddings` for the [Vertex AI text-embeddings API](https://cloud.google.com/vertex-ai/docs/generative-ai/embeddings/get-text-embeddings).

`MLTransform` is available in the Python SDK versions 2.53.0 and later. Only pipelines that use [default windows](https://beam.apache.org/documentation/programming-guide/#single-global-window) are supported. 



