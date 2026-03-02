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
from typing import Iterable
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

from google.cloud import pubsub_v1
import torch
import PIL.Image as PILImage

# ============ Utility & Preprocessing ============


def now_millis() -> int:
  return int(time.time() * 1000)


def read_gcs_file_lines(gcs_path: str) -> Iterable[str]:
  """Reads text lines from a GCS file."""
  with FileSystems.open(gcs_path) as f:
    for line in f.read().decode("utf-8").splitlines():
      yield line.strip()


def load_image_from_uri(uri: str) -> bytes:
  with FileSystems.open(uri) as f:
    return f.read()


def decode_to_tens(
    image_bytes: bytes,
    resize_shorter_side: Optional[int] = None) -> torch.Tensor:
  """Decode bytes -> RGB PIL -> optional resize -> float tensor [0..1], CHW.

  Note: TorchVision detection models apply their own normalization internally.
  """
  with PILImage.open(io.BytesIO(image_bytes)) as img:
    img = img.convert("RGB")

    if resize_shorter_side and resize_shorter_side > 0:
      w, h = img.size
      # Resize so that shorter side == resize_shorter_side, keep aspect ratio.
      if w < h:
        new_w = resize_shorter_side
        new_h = int(h * (resize_shorter_side / float(w)))
      else:
        new_h = resize_shorter_side
        new_w = int(w * (resize_shorter_side / float(h)))
      img = img.resize((new_w, new_h))

    import numpy as np
    arr = np.asarray(img).astype("float32") / 255.0  # H,W,3 in [0..1]
    arr = np.transpose(arr, (2, 0, 1))  # CHW
    return torch.from_numpy(arr)


def sha1_hex(s: str) -> str:
  import hashlib
  return hashlib.sha1(s.encode("utf-8")).hexdigest()


# ============ DoFns ============


class RateLimitDoFn(beam.DoFn):
  def __init__(self, rate_per_sec: float):
    self.delay = 1.0 / rate_per_sec

  def process(self, element):
    time.sleep(self.delay)
    yield element


class MakeKeyDoFn(beam.DoFn):
  """Produce (image_id, uri) where image_id is stable for dedup and keys."""
  def process(self, element: str):
    uri = element
    image_id = sha1_hex(uri)
    yield image_id, uri


class DecodePreprocessDoFn(beam.DoFn):
  """Turn (image_id, uri) -> (image_id, tensor)."""
  def __init__(self, resize_shorter_side: Optional[int] = None):
    self.resize_shorter_side = resize_shorter_side

  def process(self, kv: Tuple[str, str]):
    image_id, uri = kv
    start = now_millis()
    try:
      b = load_image_from_uri(uri)
      tensor = decode_to_tens(b, resize_shorter_side=self.resize_shorter_side)
      preprocess_ms = now_millis() - start
      yield image_id, {
          "tensor": tensor, "preprocess_ms": preprocess_ms, "uri": uri
      }
    except Exception as e:
      logging.warning("Decode failed for %s (%s): %s", image_id, uri, e)
      return


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
  del inference_args
  del model_id

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
    image_id, pred = kv

    # pred can be PredictionResult OR raw torchvision dict.
    if hasattr(pred, "inference"):
      inference_obj = pred.inference
    else:
      inference_obj = pred

    if isinstance(inference_obj, list) and len(inference_obj) == 1:
      inference_obj = inference_obj[0]

    if not isinstance(inference_obj, dict):
      logging.warning(
          "Unexpected inf-ce type for %s: %s", image_id, type(inference_obj))
      yield {
          "image_id": image_id,
          "model_name": self.model_name,
          "detections": json.dumps([]),
          "num_detections": 0,
          "infer_ms": now_millis(),
      }
      return

    extracted = self._extract_detection(inference_obj)

    yield {
        "image_id": image_id,
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
      '--rate_limit',
      type=float,
      default=None,
      help='Elements per second for load pipeline')

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
  parser.add_argument('--resize_shorter_side', type=int, default=0)

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

  topic_name = topic_path.split("/")[-1]
  subscription_name = subscription_path.split("/")[-1]

  full_topic_path = publisher.topic_path(project, topic_name)
  full_subscription_path = subscriber.subscription_path(
      project, subscription_name)

  try:
    publisher.get_topic(request={"topic": full_topic_path})
  except Exception:
    publisher.create_topic(name=full_topic_path)

  try:
    subscriber.get_subscription(
        request={"subscription": full_subscription_path})
  except Exception:
    subscriber.create_subscription(
        name=full_subscription_path, topic=full_topic_path)


def cleanup_pubsub_resources(
    project: str, topic_path: str, subscription_path: str):
  publisher = pubsub_v1.PublisherClient()
  subscriber = pubsub_v1.SubscriberClient()

  topic_name = topic_path.split("/")[-1]
  subscription_name = subscription_path.split("/")[-1]

  full_topic_path = publisher.topic_path(project, topic_name)
  full_subscription_path = subscriber.subscription_path(
      project, subscription_name)

  try:
    subscriber.delete_subscription(
        request={"subscription": full_subscription_path})
    print(f"Deleted subscription: {subscription_name}")
  except Exception as e:
    print(f"Failed to delete subscription: {e}")

  try:
    publisher.delete_topic(request={"topic": full_topic_path})
    print(f"Deleted topic: {topic_name}")
  except Exception as e:
    print(f"Failed to delete topic: {e}")


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

  lines = (
      pipeline
      |
      'ReadGCSFile' >> beam.Create(list(read_gcs_file_lines(known_args.input)))
      | 'FilterEmpty' >> beam.Filter(lambda line: line.strip()))
  if known_args.rate_limit:
    lines = lines | 'RateLimit' >> beam.ParDo(
        RateLimitDoFn(rate_per_sec=known_args.rate_limit))

  _ = (
      lines
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
    threading.Thread(
        target=lambda:
        (time.sleep(900), run_load_pipeline(known_args, pipeline_args)),
        daemon=True).start()

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = (
      known_args.mode == 'streaming')

  device = 'GPU' if known_args.device.upper() == 'GPU' else 'CPU'
  resize_shorter_side = (
      known_args.resize_shorter_side
  ) if known_args.resize_shorter_side > 0 else None

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
        | 'ReadURIsBatch' >> beam.Create(
            list(read_gcs_file_lines(known_args.input)))
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
          DecodePreprocessDoFn(resize_shorter_side=resize_shorter_side)))

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
  result.wait_until_finish(duration=1800000)  # 30 min
  try:
    result.cancel()
  except Exception:
    pass

  if known_args.mode == 'streaming':
    cleanup_pubsub_resources(
        project=known_args.project,
        topic_path=known_args.pubsub_topic,
        subscription_path=known_args.pubsub_subscription)

  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
