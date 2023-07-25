---
title: "Auto Update ML models using WatchFilePattern"
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

# Use WatchFilePattern to auto-update ML models in RunInference

The pipeline in this example uses a [RunInference](https://beam.apache.org/documentation/transforms/python/elementwise/runinference/) `PTransform` to run inference on images using TensorFlow models. It uses a [side input](https://beam.apache.org/documentation/programming-guide/#side-inputs) `PCollection` that emits `ModelMetadata` to update the model.

Using side inputs, you can update your model (which is passed in a `ModelHandler` configuration object) in real-time, even while the Beam pipeline is still running. This can be done either by leveraging one of Beam's provided patterns, such as the `WatchFilePattern`,
or by configuring a custom side input `PCollection` that defines the logic for the model update.

For more information about side inputs, see the [Side inputs](https://beam.apache.org/documentation/programming-guide/#side-inputs) section in the Apache Beam Programming Guide.

This example uses [`WatchFilePattern`](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.utils.html#apache_beam.ml.inference.utils.WatchFilePattern) as a side input. `WatchFilePattern` is used to watch for the file updates matching the `file_pattern`
based on timestamps. It emits the latest [`ModelMetadata`](https://beam.apache.org/documentation/transforms/python/elementwise/runinference/), which is used in
the RunInference `PTransform` to automatically update the ML model without stopping the Beam pipeline.

## Set up the source

To read the image names, use a Pub/Sub topic as the source. The Pub/Sub topic emits a `UTF-8` encoded model path that is used to read and preprocess images to run the inference.

## Models for image segmentation

For the purpose of this example, use TensorFlow models saved in [HDF5](https://www.tensorflow.org/tutorials/keras/save_and_load#hdf5_format) format.


## Pre-process images for inference
The Pub/Sub topic emits an image path. We need to read and preprocess the image to use it for RunInference. The `read_image` function is used to read the image for inference.

```python
import io
from PIL import Image
from apache_beam.io.filesystems import FileSystems
import numpy
import tensorflow as tf

def read_image(image_file_name):
  with FileSystems().open(image_file_name, 'r') as file:
    data = Image.open(io.BytesIO(file.read())).convert('RGB')
  img = data.resize((224, 224))
  img = numpy.array(img) / 255.0
  img_tensor = tf.cast(tf.convert_to_tensor(img[...]), dtype=tf.float32)
  return img_tensor
```

Now, let's jump into the pipeline code.

**Pipeline steps**:
1. Get the image names from the Pub/Sub topic.
2. Read and pre-process the images using the `read_image` function.
3. Pass the images to the RunInference `PTransform`. RunInference takes `model_handler` and `model_metadata_pcoll` as input parameters.

For the [`model_handler`](https://github.com/apache/beam/blob/07f52a478174f8733c7efedb7189955142faa5fa/sdks/python/apache_beam/ml/inference/base.py#L308), we use [TFModelHandlerTensor](https://github.com/apache/beam/blob/186973b110d82838fb8e5ba27f0225a67c336591/sdks/python/apache_beam/ml/inference/tensorflow_inference.py#L184).
```python
from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerTensor
# initialize TFModelHandlerTensor with a .h5 model saved in a directory accessible by the pipeline.
tf_model_handler = TFModelHandlerTensor(model_uri='gs://<your-bucket>/<model_path.h5>')
```

The `model_metadata_pcoll` is a [side input](https://beam.apache.org/documentation/programming-guide/#side-inputs) `PCollection` to the RunInference `PTransform`. This side input is used to update the models in the `model_handler` without needing to stop the beam pipeline.
We will use `WatchFilePattern` as side input to watch a glob pattern matching `.h5` files.

`model_metadata_pcoll` expects a `PCollection` of ModelMetadata compatible with [AsSingleton](https://beam.apache.org/releases/pydoc/2.4.0/apache_beam.pvalue.html#apache_beam.pvalue.AsSingleton). Because the pipeline uses `WatchFilePattern` as side input, it will take care of windowing and wrapping the output into `ModelMetadata`.


After the pipeline starts processing data and when you see some outputs emitted from the RunInference `PTransform`, upload a `.h5` `TensorFlow` model that matches the `file_pattern` to the Google Cloud Storage bucket. RunInference will update the `model_uri` of `TFModelHandlerTensor` using `WatchFilePattern` as a side input.

**Note**: Side input update frequency is non-deterministic and can have longer intervals between updates.

```python
import apache_beam as beam
from apache_beam.ml.inference.utils import WatchFilePattern
from apache_beam.ml.inference.base import RunInference
with beam.Pipeline() as pipeline:

  file_pattern = 'gs://<your-bucket>/*.h5'
  pubsub_topic = '<topic_emitting_image_names>'

  side_input_pcoll = (
    pipeline
    | "FilePatternUpdates" >> WatchFilePattern(file_pattern=file_pattern))

  images_pcoll = (
    pipeline
    | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=pubsub_topic)
    | "DecodeBytes" >> beam.Map(lambda x: x.decode('utf-8'))
    | "PreProcessImage" >> beam.Map(read_image)
  )

  inference_pcoll = (
    images_pcoll
    | "RunInference" >> RunInference(
    model_handler=tf_model_handler,
    model_metadata_pcoll=side_input_pcoll))

```


## Post-process the `PredictionResult` object

When the inference is complete, RunInference outputs a `PredictionResult` object that contains `example`, `inference`, and `model_id` fields. The `model_id` is used to identify which model is used for running the inference.

```python
from apache_beam.ml.inference.base import PredictionResult

class PostProcessor(beam.DoFn):
  """
  Process the PredictionResult to get the predicted label and model id used for inference.
  """
  def process(self, element: PredictionResult) -> typing.Iterable[str]:
    predicted_class = numpy.argmax(element.inference[0], axis=-1)
    labels_path = tf.keras.utils.get_file(
        'ImageNetLabels.txt',
        'https://storage.googleapis.com/download.tensorflow.org/data/ImageNetLabels.txt'
    )
    imagenet_labels = numpy.array(open(labels_path).read().splitlines())
    predicted_class_name = imagenet_labels[predicted_class]
    return predicted_class_name.title(), element.model_id

post_processor_pcoll = (inference_pcoll | "PostProcessor" >> PostProcessor())
```

## Run the pipeline
```python
result = pipeline.run().wait_until_finish()
```
**Note**: The `model_name` of the `ModelMetaData` object will be attached as prefix to the [metrics](https://beam.apache.org/documentation/ml/runinference-metrics/) calculated by the RunInference `PTransform`.

## Final remarks
You can use this example as a pattern when using side inputs with the RunInference `PTransform` to auto-update the models without stopping the pipeline. You can see a similar example for PyTorch on [GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_image_classification_with_side_inputs.py).
