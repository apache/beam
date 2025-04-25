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
TensorRT.
"""

import argparse
import io
import os
from collections.abc import Iterable
from typing import Optional

import numpy as np

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.tensorrt_inference import TensorRTEngineHandlerNumPy  # pylint: disable=line-too-long
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from PIL import Image

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
    data: tuple[str, Image.Image]) -> tuple[tuple[str, int, int], Image.Image]:
  filename, image = data
  width, height = image.size
  return ((filename, width, height), image)


def read_image(image_file_name: str,
               path_to_dir: Optional[str] = None) -> tuple[str, Image.Image]:
  if path_to_dir is not None:
    image_file_name = os.path.join(path_to_dir, image_file_name)
  with FileSystems().open(image_file_name, 'r') as file:
    data = Image.open(io.BytesIO(file.read())).convert('RGB')
    return image_file_name, data


def preprocess_image(image: Image.Image) -> np.ndarray:
  ssd_mobilenet_v2_320x320_input_dims = (300, 300)
  image = image.resize(
      ssd_mobilenet_v2_320x320_input_dims, resample=Image.Resampling.BILINEAR)
  image = np.expand_dims(np.asarray(image, dtype=np.float32), axis=0)
  return image


class PostProcessor(beam.DoFn):
  """Processes the PredictionResult that consists of
  number of detections per image, box coordinates, scores and classes.

  We loop over all detections to organize attributes on a per
  detection basis. Box coordinates are normalized, hence we have to scale them
  according to original image dimensions. Score is a floating point number
  that provides probability percentage of a particular object. Class is
  an integer that we can transform into actual string class using
  COCO_OBJ_DET_CLASSES as reference.
  """
  def process(self, element: tuple[str, PredictionResult]) -> Iterable[str]:
    key, prediction_result = element
    filename, im_width, im_height = key
    num_detections = prediction_result.inference[0]
    boxes = prediction_result.inference[1]
    scores = prediction_result.inference[2]
    classes = prediction_result.inference[3]
    detections = []
    for i in range(int(num_detections[0])):
      detections.append({
          'ymin': str(boxes[i][0] * im_height),
          'xmin': str(boxes[i][1] * im_width),
          'ymax': str(boxes[i][2] * im_height),
          'xmax': str(boxes[i][3] * im_width),
          'score': str(scores[i]),
          'class': COCO_OBJ_DET_CLASSES[int(classes[i])]
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
      '--engine_path',
      dest='engine_path',
      required=True,
      help='Path to the pre-built TFOD ssd_mobilenet_v2_320x320_coco17_tpu-8'
      'TensorRT engine.')
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

  engine_handler = KeyedModelHandler(
      TensorRTEngineHandlerNumPy(
          min_batch_size=1,
          max_batch_size=1,
          engine_path=known_args.engine_path))

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
        | 'TensorRTRunInference' >> RunInference(engine_handler)
        | 'ProcessOutput' >> beam.ParDo(PostProcessor()))

    _ = (
        predictions | "WriteOutputToGCS" >> beam.io.WriteToText(
            known_args.output,
            shard_name_template='',
            append_trailing_newlines=True))


if __name__ == '__main__':
  run()
