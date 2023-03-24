---
title: "Auto Model Updates in RunInference Transforms using SideInputs"
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

# Use Slowly-Updating Side Input Pattern to Update Models in RunInference Transform

The pipeline in this example uses RunInference PTransform with a side input PCollection that emits ModelMetadata to predict images using an open source Tensorflow model trained on ImageNet.

In this example, we will use `WatchFilePattern` as side input. `WatchFilePattern` is used to watch for a file updates matching the file_pattern
based on timestamps and emits the latest `ModelMetadata`, which is used in
RunInference PTransform for the dynamic model updates without the need for stopping
the beam pipeline.

Note: Slowly-updating side input pattern is non-deterministic.

You can find the code used in this example in the [Beam repository] (link).

## Dataset and model for image segmentation

We will use [resnet_v2_101](https://tfhub.dev/google/imagenet/resnet_v2_101/classification/5) for initial predictions. After a while, we will upload a `model_2` to the GCS bucket. The bucket path will be used a glob pattern and is passed to the WatchFilePattern.

## Setting up source.

We will use PubSub topic as a source to read the image names. 
 * PubSub emits `UTF-8` encoded path to the model to be used in RunInference PTransform.

### Custom Python transform
In addition to running inference, we also need to perform preprocessing and postprocessing on the data. 

### ModelHandler used for Predictions.

For the ModelHandler, we will be using Tensorflow model handler. (link)
```python
from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerTensor
tf_model_handler = TFModelHandlerTensor(model_uri='<model_path.h5>')
```

The PubSub topic emits an image path. We need to read and preprocess the image to use it for RunInference. `read_image` function is used to read the image for inference.

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

In the pipeline below, the `WatchFilePattern` is used as side input, which is used to watch a glob pattern matching `.h5` files. We use [HDF5](https://www.tensorflow.org/tutorials/keras/save_and_load#hdf5_format) standard to load the models
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

`model_metadata_pcoll` expects a PCollection of `ModelMetadata` compatible with AsSingleton view (link). Since we use `WatchFilePattern`, it will take care of windowing and wrapping the output into `ModelMetadata`.

### PostProcessing results

Once the inference is done, RunInference outputs `PredictionResult` object that contains `example`, `inference` and, `model_id`. Here, the `model_id` is used to identify which model is used for running the inference.

```python
from apache_beam.ml.inference.base import PredictionResult
import typing
import numpy
import apache_beam as beam
import tensorflow as tf

class PostProcessor(beam.DoFn):
  """Process the PredictionResult to get the predicted label.
  Returns predicted label.
  """
  def process(self, element: PredictionResult) -> typing.Iterable[str]:
    predicted_class = numpy.argmax(element.inference[0], axis=-1)
    labels_path = tf.keras.utils.get_file(
        'ImageNetLabels.txt',
        'https://storage.googleapis.com/download.tensorflow.org/data/ImageNetLabels.txt'  # pylint: disable=line-too-long
    )
    imagenet_labels = numpy.array(open(labels_path).read().splitlines())
    predicted_class_name = imagenet_labels[predicted_class]
    return predicted_class_name.title(), element.model_id

(inference_pcoll | "PostProcessor" >> PostProcessor())
```
