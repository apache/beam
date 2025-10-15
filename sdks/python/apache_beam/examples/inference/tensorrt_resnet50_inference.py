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

"""Example pipeline using TensorRT 10.x with ONNX model conversion.

This example demonstrates the TensorRT 10.x handler's ability to:
1. Load ONNX models directly (no pre-conversion needed)
2. Build TensorRT engines on-worker (avoids environment alignment issues)
3. Use the new Tensor API for inference

**Key Advantage over Legacy TensorRT:**
The on-worker ONNX-to-engine conversion ensures that the TensorRT engine is
built in the exact same environment where inference runs. This eliminates
compatibility issues that occur when pre-building engines on different
GPU/CUDA/TensorRT versions.

Example Usage:
  # Using ONNX model (builds engine on worker)
  python tensorrt_resnet50_inference.py \\
    --onnx_path=gs://my-bucket/resnet50.onnx \\
    --input=gs://my-bucket/images.txt \\
    --output=gs://my-bucket/predictions.txt \\
    --runner=DataflowRunner \\
    --project=my-project \\
    --region=us-central1 \\
    --experiment=worker_accelerator=type:nvidia-tesla-t4;count:1;install-nvidia-driver

  # Using pre-built engine (for optimal performance)
  python tensorrt_resnet50_inference.py \\
    --engine_path=gs://my-bucket/resnet50.engine \\
    --input=gs://my-bucket/images.txt \\
    --output=gs://my-bucket/predictions.txt
"""

import argparse
import io
import logging
import numpy as np
from typing import Iterable

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import PredictionResult, RunInference
from apache_beam.ml.inference.trt_handler_numpy_compact import \
    TensorRTEngineHandlerNumPy
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

try:
  from PIL import Image
except ImportError:
  Image = None


def read_image(image_path: str) -> tuple[str, np.ndarray]:
  """Read and preprocess image for ResNet-50 inference.

  Args:
    image_path: Path to image file (supports GCS, local, etc.)

  Returns:
    Tuple of (image_path, preprocessed_array)
  """
  if Image is None:
    raise ImportError(
        "Pillow is required for image processing. "
        "Install with: pip install pillow")

  with FileSystems().open(image_path, 'r') as f:
    img = Image.open(io.BytesIO(f.read())).convert('RGB')

  # ResNet-50 preprocessing: resize to 224x224, normalize to [0,1]
  img = img.resize((224, 224), resample=Image.Resampling.BILINEAR)
  arr = np.asarray(img, dtype=np.float32)

  # Convert HWC to CHW and add batch dimension
  arr = np.transpose(arr, (2, 0, 1))  # HWC -> CHW
  arr = np.expand_dims(arr, axis=0)  # CHW -> NCHW

  # Normalize to [0, 1]
  arr = arr / 255.0

  return image_path, arr


class FormatPrediction(beam.DoFn):
  """Format TensorRT predictions for output."""
  def process(self, element: tuple[str, PredictionResult]) -> Iterable[str]:
    """Format prediction result.

    Args:
      element: Tuple of (image_path, PredictionResult)

    Yields:
      Formatted prediction string
    """
    image_path, prediction = element

    # Extract output tensors
    outputs = prediction.inference
    if not outputs:
      yield f"{image_path},ERROR: No outputs"
      return

    # For ResNet-50, output[0] is typically (1, 1000) logits
    logits = np.asarray(outputs[0])

    if logits.size == 0:
      yield f"{image_path},ERROR: Empty output"
      return

    # Get top-5 predictions
    flat_logits = logits.flatten()
    top5_indices = np.argsort(flat_logits)[-5:][::-1]
    top5_scores = flat_logits[top5_indices]

    # Format: image_path,class1:score1,class2:score2,...
    predictions_str = ','.join(
        f"{idx}:{score:.4f}" for idx, score in zip(top5_indices, top5_scores))

    yield f"{image_path},{predictions_str}"


def parse_known_args(argv):
  """Parse command line arguments."""
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--onnx_path',
      dest='onnx_path',
      help='Path to ONNX model file. Engine will be built on worker. '
      'Use this for maximum compatibility across environments.')

  parser.add_argument(
      '--engine_path',
      dest='engine_path',
      help='Path to pre-built TensorRT engine (.engine). '
      'Provides best performance but requires matching GPU/CUDA/TensorRT versions.'
  )

  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Path to text file containing image paths (one per line).')

  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Path for output predictions.')

  parser.add_argument(
      '--min_batch_size',
      dest='min_batch_size',
      type=int,
      default=1,
      help='Minimum batch size for inference. Default: 1')

  parser.add_argument(
      '--max_batch_size',
      dest='max_batch_size',
      type=int,
      default=4,
      help='Maximum batch size for inference. Default: 4')

  parser.add_argument(
      '--max_batch_duration_secs',
      dest='max_batch_duration_secs',
      type=int,
      default=1,
      help='Maximum seconds to wait for batch to fill. Default: 1')

  return parser.parse_known_args(argv)


def run(argv=None, save_main_session=True):
  """Run the TensorRT inference pipeline.

  Args:
    argv: Command line arguments
    save_main_session: Whether to save main session for pickling
  """
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # Validate arguments
  if not known_args.onnx_path and not known_args.engine_path:
    raise ValueError("Must provide either --onnx_path or --engine_path")

  if known_args.onnx_path and known_args.engine_path:
    raise ValueError(
        "Provide only one of --onnx_path or --engine_path, not both")

  # Create handler
  if known_args.onnx_path:
    logging.info(
        "Using ONNX model: %s (will build engine on worker)",
        known_args.onnx_path)
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=known_args.min_batch_size,
        max_batch_size=known_args.max_batch_size,
        max_batch_duration_secs=known_args.max_batch_duration_secs,
        onnx_path=known_args.onnx_path,
        build_on_worker=True,  # Key feature: builds in worker environment!
    )
  else:
    logging.info("Using pre-built engine: %s", known_args.engine_path)
    handler = TensorRTEngineHandlerNumPy(
        min_batch_size=known_args.min_batch_size,
        max_batch_size=known_args.max_batch_size,
        max_batch_duration_secs=known_args.max_batch_duration_secs,
        engine_path=known_args.engine_path,
    )

  with beam.Pipeline(options=pipeline_options) as p:
    predictions = (
        p
        | 'ReadImagePaths' >> beam.io.ReadFromText(known_args.input)
        | 'LoadAndPreprocess' >> beam.Map(read_image)
        | 'ExtractArrays' >> beam.MapTuple(lambda path, arr: (path, arr))
        | 'RunTensorRT' >> RunInference(handler)
        | 'FormatOutput' >> beam.ParDo(FormatPrediction()))

    _ = predictions | 'WriteResults' >> beam.io.WriteToText(
        known_args.output,
        shard_name_template='',
        append_trailing_newlines=True)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
