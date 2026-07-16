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

"""This pipeline performs object detection using an open-source PyTorch
TorchVision detection model (e.g., Faster R-CNN ResNet50 FPN) on GPU.

It reads image URIs from a GCS input file, decodes and preprocesses images,
runs batched GPU inference via RunInference, post-processes detection outputs,
and writes results to BigQuery.

The pipeline targets stable and reproducible performance measurements for
GPU inference workloads (no right-fitting; fixed batch size).
"""

import argparse
import io
import json
import logging
import threading
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms import window

from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1
import torch
import PIL.Image as PILImage

# ============ Utility & Preprocessing ============


def now_millis() -> int:
  return int(time.time() * 1000)


def decode_to_tens(image_bytes: bytes, image_size: int = 800) -> torch.Tensor:
  """Decode bytes -> RGB PIL -> resize/pad square -> float tensor [0..1], CHW.

  TorchVision detection models accept float tensors in [0..1]. We force a fixed
  square shape so PytorchModelHandlerTensor can batch tensors with torch.stack.
  """
  with PILImage.open(io.BytesIO(image_bytes)) as img:
    img = img.convert("RGB")

    w, h = img.size
    scale = min(image_size / float(w), image_size / float(h))
    new_w = max(1, int(round(w * scale)))
    new_h = max(1, int(round(h * scale)))

    img = img.resize((new_w, new_h))

    padded = PILImage.new("RGB", (image_size, image_size), color=(0, 0, 0))
    left = (image_size - new_w) // 2
    top = (image_size - new_h) // 2
    padded.paste(img, (left, top))

    import numpy as np
    arr = np.asarray(padded).astype("float32") / 255.0
    arr = np.transpose(arr, (2, 0, 1))
    return torch.from_numpy(arr).float()


# ============ DoFns ============


class MakeKeyDoFn(beam.DoFn):
  """Produce (uri, uri) where the URI is used as the stable key."""
  def process(self, element: str):
    uri = element
    yield uri, uri


class DecodePreprocessDoFn(beam.DoFn):
  """Turn (uri, uri) -> (uri, tensor)."""
  def __init__(self, image_size: int = 800):
    self.image_size = image_size

  def process(self, kv: Tuple[str, str]):
    uri, _ = kv
    start = now_millis()
    try:
      with FileSystems.open(uri) as f:
        image_bytes = f.read()
      tensor = decode_to_tens(image_bytes, image_size=self.image_size)
      preprocess_ms = now_millis() - start
      yield uri, {"tensor": tensor, "preprocess_ms": preprocess_ms}
    except (OSError, ValueError):
      logging.exception("Decode failed for %s", uri)
      return


# pylint: disable=unused-argument
def _torchvision_detection_inference_fn(
    batch: Sequence[torch.Tensor],
    model: torch.nn.Module,
    device: torch.device,
    inference_args: Optional[dict[str, Any]] = None,
    model_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
  """Inference function for TorchVision detection models.

  TorchVision detection models expect List[Tensor] where each tensor is:
    - shape: [3, H, W]
    - dtype: float32
    - values: [0..1]
  """

  with torch.no_grad():
    # Ensure tensors are on device
    inputs = []
    for t in batch:
      if isinstance(t, torch.Tensor):
        inputs.append(t.to(device))
      else:
        # Defensive: if somehow non-tensor slips through.
        inputs.append(torch.as_tensor(t).to(device))
    outputs = model(inputs)  # List[Dict[str, Tensor]]
    return outputs


class PostProcessDoFn(beam.DoFn):
  """PredictionResult -> dict row for BQ."""
  def __init__(
      self, model_name: str, score_threshold: float, max_detections: int):
    self.model_name = model_name
    self.score_threshold = score_threshold
    self.max_detections = max_detections

  def _extract_detection(self, inference_obj: Any) -> Dict[str, Any]:
    """Extract detection fields from torchvision output dict."""
    # Expect: {'boxes': Tensor[N,4], 'labels': Tensor[N], 'scores': Tensor[N]}
    boxes = inference_obj.get("boxes")
    labels = inference_obj.get("labels")
    scores = inference_obj.get("scores")

    # Convert to CPU lists
    if isinstance(scores, torch.Tensor):
      scores_list = scores.detach().cpu().tolist()
    else:
      scores_list = list(scores) if scores is not None else []

    if isinstance(labels, torch.Tensor):
      labels_list = labels.detach().cpu().tolist()
    else:
      labels_list = list(labels) if labels is not None else []

    if isinstance(boxes, torch.Tensor):
      boxes_list = boxes.detach().cpu().tolist()
    else:
      boxes_list = list(boxes) if boxes is not None else []

    # Filter by threshold and trim to max_detections
    dets = []
    for i in range(min(len(scores_list), len(labels_list), len(boxes_list))):
      score = float(scores_list[i])
      if score < self.score_threshold:
        continue
      box = boxes_list[i]  # [x1,y1,x2,y2]
      dets.append({
          "label_id": int(labels_list[i]),
          "score": score,
          "box": [float(box[0]), float(box[1]), float(box[2]), float(box[3])],
      })
      if len(dets) >= self.max_detections:
        break

    return {
        "detections": dets,
        "num_detections": len(dets),
    }

  def process(self, kv: Tuple[str, PredictionResult]):
    image_uri, pred = kv

    # pred can be PredictionResult OR raw torchvision dict.
    if hasattr(pred, "inference"):
      inference_obj = pred.inference
    else:
      inference_obj = pred

    if isinstance(inference_obj, list) and len(inference_obj) == 1:
      inference_obj = inference_obj[0]

    if not isinstance(inference_obj, dict):
      logging.warning(
          "Unexpected inf-ce type for %s: %s", image_uri, type(inference_obj))
      yield {
          "image_id": image_uri,
          "model_name": self.model_name,
          "detections": json.dumps([]),
          "num_detections": 0,
          "infer_ms": now_millis(),
      }
      return

    extracted = self._extract_detection(inference_obj)

    yield {
        "image_id": image_uri,
        "model_name": self.model_name,
        "detections": json.dumps(extracted["detections"]),
        "num_detections": int(extracted["num_detections"]),
        "infer_ms": now_millis(),
    }


# ============ Args & Helpers ============


def parse_known_args(argv):
  parser = argparse.ArgumentParser()

  # I/O & runtime
  parser.add_argument(
      '--project', default='apache-beam-testing', help='GCP project ID')
  parser.add_argument(
      '--mode', default='streaming', choices=['streaming', 'batch'])
  parser.add_argument(
      '--output_table',
      required=True,
      help='BigQuery output table: dataset.table')
  parser.add_argument(
      '--publish_to_big_query', default='true', choices=['true', 'false'])
  parser.add_argument(
      '--input', required=True, help='GCS path to file with image URIs')
  parser.add_argument(
      '--pubsub_topic',
      default='projects/apache-beam-testing/topics/images_topic')
  parser.add_argument(
      '--pubsub_subscription',
      default='projects/apache-beam-testing/subscriptions/images_subscription')
  parser.add_argument(
      '--feeder_start_delay_sec',
      type=int,
      default=900,
      help=(
          'Delay before starting the feeder pipeline that reads URIs from GCS '
          'and publishes them to Pub/Sub. This delay allows the main streaming '
          'pipeline workers to start and scale before data ingestion begins.'),
  )

  # Model & inference
  parser.add_argument(
      '--pretrained_model_name',
      default='fasterrcnn_resnet50_fpn',
      help=(
          'TorchVision detection model name '
          '(e.g., fasterrcnn_resnet50_fpn)'))
  parser.add_argument(
      '--model_state_dict_path',
      required=True,
      help='GCS path to a state_dict .pth for the chosen model')
  parser.add_argument('--device', default='GPU', choices=['CPU', 'GPU'])

  # Batch sizing (no right-fitting)
  parser.add_argument('--inference_batch_size', type=int, default=8)

  # Preprocess
  parser.add_argument('--image_size', type=int, default=800)

  # Postprocess
  parser.add_argument('--score_threshold', type=float, default=0.5)
  parser.add_argument('--max_detections', type=int, default=50)

  # Windows
  parser.add_argument('--window_sec', type=int, default=60)
  parser.add_argument('--trigger_proc_time_sec', type=int, default=30)

  known_args, pipeline_args = parser.parse_known_args(argv)
  return known_args, pipeline_args


def ensure_pubsub_resources(
    project: str, topic_path: str, subscription_path: str):
  publisher = pubsub_v1.PublisherClient()
  subscriber = pubsub_v1.SubscriberClient()

  try:
    publisher.get_topic(request={"topic": topic_path})
  except NotFound:
    publisher.create_topic(name=topic_path)

  try:
    subscriber.get_subscription(
      request={"subscription": subscription_path})
  except NotFound:
    subscriber.create_subscription(
      name=subscription_path, topic=topic_path)


def cleanup_pubsub_resources(
    project: str, topic_path: str, subscription_path: str):
  publisher = pubsub_v1.PublisherClient()
  subscriber = pubsub_v1.SubscriberClient()

  try:
    subscriber.delete_subscription(
      request={"subscription": subscription_path})
    logging.info(f"Deleted subscription: {subscription_path}")
  except NotFound:
    logging.info(f"Subscription already deleted: {subscription_path}")

  try:
    publisher.delete_topic(request={"topic": topic_path})
    logging.info(f"Deleted topic: {topic_path}")
  except NotFound:
    logging.info(f"Topic already deleted: {topic_path}")


def override_or_add(args, flag, value):
  if flag in args:
    idx = args.index(flag)
    args[idx + 1] = str(value)
  else:
    args.extend([flag, str(value)])


def create_torchvision_detection_model(model_name: str):
  """Creates a TorchVision detection model instance.

  Note: We will load weights via state_dict_path (required by Beam handler when
  model_class is provided).
  """
  import torchvision

  name = model_name.strip()

  if name == "fasterrcnn_resnet50_fpn":
    model = torchvision.models.detection.fasterrcnn_resnet50_fpn(weights=None)
  elif name == "retinanet_resnet50_fpn":
    model = torchvision.models.detection.retinanet_resnet50_fpn(weights=None)
  else:
    raise ValueError(f"Unsupported detection model: {model_name}")

  model.eval()
  return model


# ============ Load pipeline ============


def run_load_pipeline(known_args, pipeline_args):
  """Reads GCS file with URIs and publishes them to Pub/Sub (for streaming)."""

  pipeline_args = list(pipeline_args)
  # enforce smaller/CPU-only defaults for feeder
  override_or_add(pipeline_args, '--device', 'CPU')
  override_or_add(pipeline_args, '--num_workers', '5')
  override_or_add(pipeline_args, '--max_num_workers', '10')
  override_or_add(
      pipeline_args, '--job_name', f"images-load-pubsub-{int(time.time())}")
  override_or_add(pipeline_args, '--project', known_args.project)
  pipeline_args = [
      arg for arg in pipeline_args if not arg.startswith("--experiments")
  ]

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline = beam.Pipeline(options=pipeline_options)

  _ = (
      pipeline
      | 'ReadGCSFile' >> beam.io.ReadFromText(known_args.input)
      | 'FilterEmpty' >> beam.Filter(lambda line: line.strip())
      | 'ToBytes' >> beam.Map(lambda line: line.encode('utf-8'))
      | 'ToPubSub' >> beam.io.WriteToPubSub(topic=known_args.pubsub_topic))
  return pipeline.run()


# ============ Main pipeline ============


def run(
    argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
  known_args, pipeline_args = parse_known_args(argv)

  if known_args.mode == 'streaming':
    ensure_pubsub_resources(
        project=known_args.project,
        topic_path=known_args.pubsub_topic,
        subscription_path=known_args.pubsub_subscription)

    # Start feeder thread that reads URIs from GCS and fills Pub/Sub.
    # Delay is used to allow the main streaming pipeline workers to start
    # and autoscale before the feeder pipeline begins publishing messages.
    threading.Thread(
        target=lambda: (
            time.sleep(known_args.feeder_start_delay_sec), run_load_pipeline(
                known_args, pipeline_args)),
        daemon=True).start()

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = (
      known_args.mode == 'streaming')

  device = 'GPU' if known_args.device.upper() == 'GPU' else 'CPU'

  # Fixed batch size (no right-fitting)
  batch_size = int(known_args.inference_batch_size)

  model_handler = PytorchModelHandlerTensor(
      model_class=lambda: create_torchvision_detection_model(
          known_args.pretrained_model_name),
      model_params={},
      state_dict_path=known_args.model_state_dict_path,
      device=device,
      inference_batch_size=batch_size,
      inference_fn=_torchvision_detection_inference_fn,
  )

  pipeline = test_pipeline or beam.Pipeline(options=pipeline_options)

  if known_args.mode == 'batch':
    pcoll = (
        pipeline
        | 'ReadURIsBatch' >> beam.io.ReadFromText(known_args.input)
        | 'FilterEmptyBatch' >> beam.Filter(lambda s: s.strip()))
  else:
    pcoll = (
        pipeline
        | 'ReadFromPubSub' >>
        beam.io.ReadFromPubSub(subscription=known_args.pubsub_subscription)
        | 'DecodeUTF8' >> beam.Map(lambda x: x.decode('utf-8'))
        | 'Window' >> beam.WindowInto(
            window.FixedWindows(known_args.window_sec),
            trigger=beam.trigger.AfterProcessingTime(
                known_args.trigger_proc_time_sec),
            accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
            allowed_lateness=0))

  keyed = (pcoll | 'MakeKey' >> beam.ParDo(MakeKeyDoFn()))

  preprocessed = (
      keyed
      | 'DecodePreprocess' >> beam.ParDo(
          DecodePreprocessDoFn(image_size=known_args.image_size)))

  to_infer = (
      preprocessed
      | 'ToKeyedTensor' >> beam.Map(lambda kv: (kv[0], kv[1]["tensor"])))

  predictions = (
      to_infer
      | 'RunInference' >> RunInference(KeyedModelHandler(model_handler)))

  results = (
      predictions
      | 'PostProcess' >> beam.ParDo(
          PostProcessDoFn(
              model_name=known_args.pretrained_model_name,
              score_threshold=known_args.score_threshold,
              max_detections=known_args.max_detections)))

  method = (
      beam.io.WriteToBigQuery.Method.FILE_LOADS if known_args.mode == 'batch'
      else beam.io.WriteToBigQuery.Method.STREAMING_INSERTS)

  if known_args.publish_to_big_query == 'true':
    _ = (
        results
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema=(
                'image_id:STRING, model_name:STRING, '
                'detections:STRING, num_detections:INT64, infer_ms:INT64'),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=method))

  result = pipeline.run()
  try:
    result.wait_until_finish(duration=1800000)  # 30 min
  finally:
    try:
      result.cancel()
    except Exception:
      logging.debug("Failed to cancel pipeline result.", exc_info=True)

    if known_args.mode == 'streaming':
      cleanup_pubsub_resources(
          project=known_args.project,
          topic_path=known_args.pubsub_topic,
          subscription_path=known_args.pubsub_subscription)

  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
