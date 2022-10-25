#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A pipeline that uses RunInference API to perform object detection with
TensorFlow.
"""

import argparse
import io
import os
from typing import Iterable
from typing import Optional
from typing import Tuple

import numpy as np
import tensorflow as tf

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from PIL import Image
from tfx_bsl.public.beam.run_inference import CreateModelHandler
from tfx_bsl.public.beam.run_inference import prediction_log_pb2
from tfx_bsl.public.proto import model_spec_pb2

COCO_OBJ_DET_CLASSES = [
    'person',
    'bicycle',
    'car',
    'motorcycle',
    'airplane',
    'bus',
    'train',
    'truck',
    'boat',
    'traffic light',
    'fire hydrant',
    'street sign',
    'stop sign',
    'parking meter',
    'bench',
    'bird',
    'cat',
    'dog',
    'horse',
    'sheep',
    'cow',
    'elephant',
    'bear',
    'zebra',
    'giraffe',
    'hat',
    'backpack',
    'umbrella',
    'shoe',
    'eye glasses',
    'handbag',
    'tie',
    'suitcase',
    'frisbee',
    'skis',
    'snowboard',
    'sports ball',
    'kite',
    'baseball bat',
    'baseball glove',
    'skateboard',
    'surfboard',
    'tennis racket',
    'bottle',
    'plate',
    'wine glass',
    'cup',
    'fork',
    'knife',
    'spoon',
    'bowl',
    'banana',
    'apple',
    'sandwich',
    'orange',
    'broccoli',
    'carrot',
    'hot dog',
    'pizza',
    'donut',
    'cake',
    'chair',
    'couch',
    'potted plant',
    'bed',
    'mirror',
    'dining table',
    'window',
    'desk',
    'toilet',
    'door',
    'tv',
    'laptop',
    'mouse',
    'remote',
    'keyboard',
    'cell phone',
    'microwave',
    'oven',
    'toaster',
    'sink',
    'refrigerator',
    'blender',
    'book',
    'clock',
    'vase',
    'scissors',
    'teddy bear',
    'hair drier',
    'toothbrush',
    'hair brush',
]


def attach_im_size_to_key(
    data: Tuple[str, Image.Image]) -> Tuple[Tuple[str, int, int], Image.Image]:
  filename, image = data
  width, height = image.size
  return ((filename, width, height), image)


def read_image(image_file_name: str,
               path_to_dir: Optional[str] = None) -> Tuple[str, Image.Image]:
  if path_to_dir is not None:
    image_file_name = os.path.join(path_to_dir, image_file_name)
  with FileSystems().open(image_file_name, 'r') as file:
    data = Image.open(io.BytesIO(file.read())).convert('RGB')
    return image_file_name, data


def preprocess_image(image: Image.Image) -> np.ndarray:
  ssd_mobilenet_v2_320x320_input_dims = (300, 300)
  image = image.resize(
      ssd_mobilenet_v2_320x320_input_dims, resample=Image.BILINEAR)
  image = np.asarray(image, dtype=np.uint8)
  return image


def convert_image_to_example_proto(tensor):
  """
  This method performs the following:
  1. Accepts the tensor as input
  2. Serializes the tensor into bytes and pass it through
        tf.train.Feature
  3. Pass the serialized tensor feature using tf.train.Example
      Proto to the RunInference transform.
  Args:
    tensor: A TF tensor.
  Returns:
    example_proto: A tf.train.Example containing serialized tensor.
  """
  serialized_non_scalar = tf.io.serialize_tensor(tensor)
  feature_of_bytes = tf.train.Feature(
      bytes_list=tf.train.BytesList(value=[serialized_non_scalar.numpy()]))
  features_for_example = {'image': feature_of_bytes}
  example_proto = tf.train.Example(
      features=tf.train.Features(feature=features_for_example))
  return example_proto


class ProcessInferenceToString(beam.DoFn):
  def process(
      self, element: Tuple[str,
                           prediction_log_pb2.PredictionLog]) -> Iterable[str]:
    """
    Args:
      element: Tuple of str, and PredictionLog. Inference can be parsed
        from prediction_log
    returns:
      str of filename and inference.
    """
    key, predict_log = element[0], element[1].predict_log
    filename, im_width, im_height = key

    output_value = predict_log.response.outputs
    num_detections = (output_value['num_detections'].float_val[0])
    boxes = (
        tf.io.decode_raw(
            output_value['detection_boxes'].tensor_content,
            out_type=tf.float32).numpy())
    scores = (
        tf.io.decode_raw(
            output_value['detection_scores'].tensor_content,
            out_type=tf.float32).numpy())
    # classes = (
    #     tf.io.decode_raw(
    #         output_value['detection_classes'].tensor_content,
    #         out_type=tf.int8).numpy())
    detections = []
    for i in range(int(num_detections)):
      detections.append({
          'ymin': str(boxes[i] * im_height),
          'xmin': str(boxes[i + 1] * im_width),
          'ymax': str(boxes[i + 2] * im_height),
          'xmax': str(boxes[i + 3] * im_width),
          'score': str(scores[i]),
          # 'class': COCO_OBJ_DET_CLASSES[int(classes[i])]
      })
    yield filename + ',' + str(detections)


def parse_known_args(argv):
  """Parses args for the workflow."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Path to the text file containing image names.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path where to save output predictions.'
      ' text file.')
  parser.add_argument(
      '--model_path',
      dest='model_path',
      required=True,
      help='Path to the TensorFlow saved model.')
  parser.add_argument(
      '--images_dir',
      default=None,
      help='Path to the directory where images are stored.'
      'Not required if image names in the input file have absolute path.')
  return parser.parse_known_args(argv)


def run(argv=None, save_main_session=True):
  """
  Args:
    argv: Command line arguments defined for this example.
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  saved_model_spec = model_spec_pb2.SavedModelSpec(
      model_path=known_args.model_path)
  inference_spec_type = model_spec_pb2.InferenceSpecType(
      saved_model_spec=saved_model_spec)
  tf_model_handler = CreateModelHandler(inference_spec_type)
  tf_keyed_model_handler = KeyedModelHandler(tf_model_handler)

  def set_batch_size(min_batch_size=1, max_batch_size=1):
    return {'min_batch_size': min_batch_size, 'max_batch_size': max_batch_size}

  # set the batch size until tfx adds **kwargs passed to the CreateModelHandler
  tf_model_handler.batch_elements_kwargs = set_batch_size
  tf_keyed_model_handler = KeyedModelHandler(tf_model_handler)

  with beam.Pipeline(options=pipeline_options) as p:
    filename_value_pair = (
        p
        | 'ReadImageNames' >> beam.io.ReadFromText(known_args.input)
        | 'ReadImageData' >> beam.Map(
            lambda image_name: read_image(
                image_file_name=image_name, path_to_dir=known_args.images_dir))
        | 'AttachImageSizeToKey' >> beam.Map(attach_im_size_to_key)
        | 'PreprocessImages' >> beam.MapTuple(
            lambda file_name, data: (file_name, preprocess_image(data))))
    predictions = (
        filename_value_pair
        | 'ConvertToExampleProto' >>
        beam.Map(lambda x: (x[0], convert_image_to_example_proto(x[1])))
        | 'TensorFlowRunInference' >> RunInference(tf_keyed_model_handler)
        | 'PostProcess' >> beam.ParDo(ProcessInferenceToString()))

    _ = (
        predictions | "WriteOutputToGCS" >> beam.io.WriteToText(
            known_args.output,
            shard_name_template='',
            append_trailing_newlines=True))


if __name__ == '__main__':
  run()
