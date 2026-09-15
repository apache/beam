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

"""Rebuilds the TensorRT engines that the TensorRT tests load from GCS.

A serialized TensorRT engine can only be deserialized by the TensorRT major
version and GPU architecture that built it. The engines the tests load are
therefore not portable, and have to be rebuilt whenever the TensorRT version
in the test container changes or the tests move to a different GPU.

This script rebuilds them from the ONNX sources already staged alongside them,
and verifies each result by loading it back through the model handler the
tests use. It cannot run as part of the test suite because it needs a GPU.

Run it in the same container and on the same GPU type the tests use. See
README.md in this directory for the exact commands. Requires TensorRT 10 or
later; the engines built by TensorRT 8 are the ones being replaced.
"""

# pytype: skip-file

import argparse
import io
import logging
import os
import sys
import tempfile

import numpy as np
import tensorrt as trt

TRT_LOGGER = trt.Logger(trt.Logger.INFO)
TRT_MAJOR = int(trt.__version__.split('.')[0])

SOURCE = 'gs://apache-beam-ml/models'
COCO_IMAGES = [
    'gs://apache-beam-ml/datasets/coco/raw-data/val2017/000000289594.jpg',
    'gs://apache-beam-ml/datasets/coco/raw-data/val2017/000000000139.jpg',
]


def _copy(src, dst):
  """Copies between any two paths Beam's FileSystems understands.

  The TensorRT container has no gcloud CLI, but apache-beam[gcp] is installed
  for the verification step anyway, so reuse it rather than shelling out.
  """
  from apache_beam.io.filesystems import FileSystems
  logging.info('copy %s -> %s', src, dst)
  with FileSystems.open(src) as fin, FileSystems.create(dst) as fout:
    while True:
      chunk = fin.read(8 << 20)
      if not chunk:
        break
      fout.write(chunk)


def build_engine(onnx_path, engine_path):
  """Parses an ONNX file and serializes an engine for this GPU."""
  # The SSD MobileNet ONNX contains an EfficientNMS_TRT node, so the bundled
  # plugins have to be registered before the parser will accept it.
  trt.init_libnvinfer_plugins(TRT_LOGGER, namespace="")

  builder = trt.Builder(TRT_LOGGER)
  # Explicit batch is the default from TensorRT 10 onwards, so no creation
  # flags are needed. Engines are only ever rebuilt for the container the
  # tests currently use, so there is no reason to support TensorRT 8 here.
  network = builder.create_network()
  parser = trt.OnnxParser(network, TRT_LOGGER)
  with open(onnx_path, 'rb') as f:
    if not parser.parse(f.read()):
      for i in range(parser.num_errors):
        logging.error(parser.get_error(i))
      raise ValueError(f'Failed to parse {onnx_path}')

  config = builder.create_builder_config()
  plan = builder.build_serialized_network(network, config)
  if plan is None:
    raise RuntimeError(f'Engine build produced no plan for {onnx_path}')
  with open(engine_path, 'wb') as f:
    f.write(plan)
  logging.info('built %s (%d bytes)', engine_path, os.path.getsize(engine_path))


def _handler(engine_path, batch_size):
  from apache_beam.ml.inference.tensorrt_inference import (
      TensorRTEngineHandlerNumPy)
  return TensorRTEngineHandlerNumPy(
      min_batch_size=batch_size,
      max_batch_size=batch_size,
      engine_path=engine_path)


def verify_linear(engine_path, examples, expected):
  """Checks a small linear engine against the values the unit tests assert."""
  handler = _handler(engine_path, len(examples))
  results = handler.run_inference(list(examples), handler.load_model())
  actual = np.array([r.inference[0] for r in results]).reshape(-1)
  if not np.allclose(actual, np.asarray(expected).reshape(-1), atol=1e-4):
    raise AssertionError(f'{engine_path}: expected {expected}, got {actual}')
  logging.info('verified %s -> %s', os.path.basename(engine_path), actual)


def verify_ssd(engine_path):
  """Runs the object detection engine on the images the Dataflow IT uses.

  The outputs are checked for the shape and ordering the example's
  PostProcessor indexes by, and for at least one confident detection.
  """
  from apache_beam.io.filesystems import FileSystems
  from PIL import Image

  handler = _handler(engine_path, 1)
  engine = handler.load_model()

  for image_path in COCO_IMAGES:
    with FileSystems.open(image_path) as f:
      image = Image.open(io.BytesIO(f.read())).convert('RGB')
    # Mirrors preprocess_image() in the tensorrt_object_detection example.
    image = image.resize((300, 300), resample=Image.Resampling.BILINEAR)
    batch = [np.expand_dims(np.asarray(image, dtype=np.float32), axis=0)]

    inference = list(handler.run_inference(batch, engine))[0].inference
    if len(inference) != 4:
      raise AssertionError(
          f'{engine_path}: expected 4 outputs, got {len(inference)}')
    _, boxes, scores, classes = inference
    if boxes.shape[-1] != 4 or scores.shape != classes.shape:
      raise AssertionError(
          f'{engine_path}: unexpected output shapes; the engine tensor order '
          f'must be num_detections, boxes, scores, classes. Got '
          f'{[np.asarray(o).shape for o in inference]}')
    if float(np.max(scores)) < 0.3:
      raise AssertionError(
          f'{engine_path}: no confident detection for {image_path}; top score '
          f'was {float(np.max(scores)):.3f}')
    logging.info(
        'verified %s on %s -> top score %.2f',
        os.path.basename(engine_path),
        os.path.basename(image_path),
        float(np.max(scores)))


# The inputs and outputs below mirror the constants in
# apache_beam/ml/inference/tensorrt_inference_test.py, so a rebuilt engine is
# checked against exactly what the tests will assert.
SINGLE_EXAMPLES = [np.float32(v) for v in (1, 5, -3, 10)]
SINGLE_EXPECTED = [2.5, 10.5, -5.5, 20.5]  # y = 2x + 0.5

MULTI_EXAMPLES = np.array([[1, 5], [3, 10], [-14, 0], [0.5, 0.5]],
                          dtype=np.float32)
MULTI_EXPECTED = [17.5, 36.5, -27.5, 3.0]  # y = 2*x0 + 3*x1 + 0.5


def verify_single(engine_path):
  verify_linear(engine_path, SINGLE_EXAMPLES, SINGLE_EXPECTED)


def verify_multiple(engine_path):
  verify_linear(engine_path, MULTI_EXAMPLES, MULTI_EXPECTED)


# Each entry rebuilds one staged .trt file from its staged .onnx source.
ENGINES = {
    'single_tensor_features_engine': {
        'onnx': 'single_tensor_features_model.onnx',
        'verify': verify_single,
    },
    'multiple_tensor_features_engine': {
        'onnx': 'multiple_tensor_features_model.onnx',
        'verify': verify_multiple,
    },
    'ssd_mobilenet_v2_320x320_coco17_tpu-8': {
        'onnx': 'ssd_mobilenet_v2_320x320_coco17_tpu-8.onnx',
        'verify': verify_ssd,
    },
}


def main(argv=None):
  parser = argparse.ArgumentParser(
      description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument(
      '--dest',
      required=True,
      help='Where to write the rebuilt engines, e.g. gs://my-bucket/models or '
      'a local directory. Staging them under the shared bucket is a separate, '
      'deliberate step for someone with write access.')
  parser.add_argument(
      '--suffix',
      default=None,
      help='Name suffix for the rebuilt engines, so the engines built by '
      'earlier TensorRT versions can stay in place. Defaults to _trt<major>.')
  parser.add_argument(
      '--only',
      action='append',
      choices=sorted(ENGINES),
      help='Rebuild only the named engine. May be repeated. Defaults to all.')
  args = parser.parse_args(argv)

  if args.dest.rstrip('/') == SOURCE:
    parser.error(
        f'Refusing to write to {SOURCE}. That bucket has no object '
        'versioning, so overwriting a staged engine could not be undone.')

  suffix = args.suffix if args.suffix is not None else f'_trt{TRT_MAJOR}'
  names = args.only or sorted(ENGINES)
  logging.info(
      'TensorRT %s, suffix %r, building: %s',
      trt.__version__,
      suffix,
      ', '.join(names))

  written = []
  with tempfile.TemporaryDirectory() as tmp:
    for name in names:
      spec = ENGINES[name]
      onnx_local = os.path.join(tmp, spec['onnx'])
      _copy(f'{SOURCE}/{spec["onnx"]}', onnx_local)

      engine_local = os.path.join(tmp, f'{name}{suffix}.trt')
      build_engine(onnx_local, engine_local)
      spec['verify'](engine_local)

      dest = f'{args.dest.rstrip("/")}/{os.path.basename(engine_local)}'
      _copy(engine_local, dest)
      written.append(dest)

  print('\nRebuilt and verified with TensorRT %s:' % trt.__version__)
  for dest in written:
    print(f'  {dest}')


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
  sys.exit(main())
