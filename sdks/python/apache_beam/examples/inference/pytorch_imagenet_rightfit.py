# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This streaming pipeline performs image classification using an open-source
PyTorch EfficientNet-B0 model optimized for T4 GPUs.
It reads image URIs from Pub/Sub, decodes and preprocesses them in parallel,
and runs inference with adaptive batch sizing for optimal GPU utilization.
The pipeline ensures exactly-once semantics via stateful deduplication and
idempotent BigQuery writes, allowing stable and reproducible performance
measurements under continuous load.
Resources like Pub/Sub topic/subscription cleanup is handled programmatically.
"""

import argparse
import io
import json
import logging
import threading
import time
from typing import Iterable
from typing import Optional
from typing import Tuple

import torch
import torch.nn.functional as F

import apache_beam as beam
from apache_beam.coders import BytesCoder
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms import userstate
from apache_beam.transforms import window

from google.cloud import pubsub_v1
import PIL.Image as PILImage

# ============ Utility & Preprocessing ============

IMAGENET_MEAN = [0.485, 0.456, 0.406]
IMAGENET_STD = [0.229, 0.224, 0.225]


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


def decode_and_preprocess(image_bytes: bytes, size: int = 224) -> torch.Tensor:
  """Decode bytes->RGB PIL->resize/crop->tensor->normalize."""
  with PILImage.open(io.BytesIO(image_bytes)) as img:
    img = img.convert("RGB")
    img.thumbnail((256, 256))
    w, h = img.size
    left = (w - size) // 2
    top = (h - size) // 2
    img = img.crop(
        (max(0, left), max(0, top), min(w, left + size), min(h, top + size)))

    # To tensor [0..1]
    import numpy as np
    arr = np.asarray(img).astype("float32") / 255.0  # H,W,3
    # Normalize
    arr = (arr - IMAGENET_MEAN) / IMAGENET_STD
    # HWC -> CHW
    arr = np.transpose(arr, (2, 0, 1))
    return torch.from_numpy(arr)  # float32, shape (3,224,224)


class RateLimitDoFn(beam.DoFn):
  def __init__(self, rate_per_sec: float):
    self.delay = 1.0 / rate_per_sec

  def process(self, element):
    time.sleep(self.delay)
    yield element


class MakeKeyDoFn(beam.DoFn):
  """Produce (image_id, payload) stable for dedup & BQ insertId."""
  def __init__(self, input_mode: str):
    self.input_mode = input_mode

  def process(self, element: str | bytes):
    # Input can be raw bytes from Pub/Sub or a GCS URI string, depends on mode
    if self.input_mode == "bytes":
      # element is bytes message, assume it includes
      # {"image_id": "...", "bytes": base64?} or just raw bytes.
      import hashlib
      b = element if isinstance(element, (bytes, bytearray)) else bytes(element)
      image_id = hashlib.sha1(b).hexdigest()
      yield image_id, b
    else:
      # gcs_uris: element is uri string; image_id = sha1(uri)
      import hashlib
      uri = element.decode("utf-8") if isinstance(
          element, (bytes, bytearray)) else str(element)
      image_id = hashlib.sha1(uri.encode("utf-8")).hexdigest()
      yield image_id, uri


class DedupDoFn(beam.DoFn):
  seen = userstate.ReadModifyWriteStateSpec('seen', BytesCoder())

  def process(self, element, seen=beam.DoFn.StateParam(seen)):
    if seen.read() == b'1':
      return
    seen.write(b'1')
    yield element


class DecodePreprocessDoFn(beam.DoFn):
  """Turn (image_id, bytes|uri) -> (image_id, torch.Tensor)"""
  def __init__(
      self, input_mode: str, image_size: int = 224, decode_threads: int = 4):
    self.input_mode = input_mode
    self.image_size = image_size
    self.decode_threads = decode_threads

  def process(self, kv: Tuple[str, object]):
    image_id, payload = kv
    start = now_millis()

    try:
      if self.input_mode == "bytes":
        b = payload if isinstance(payload,
                                  (bytes, bytearray)) else bytes(payload)
      else:
        uri = payload if isinstance(payload, str) else payload.decode("utf-8")
        b = load_image_from_uri(uri)

      tensor = decode_and_preprocess(b, self.image_size)
      preprocess_ms = now_millis() - start
      yield image_id, {"tensor": tensor, "preprocess_ms": preprocess_ms}
    except Exception as e:
      logging.warning("Decode failed for %s: %s", image_id, e)
      return


class PostProcessDoFn(beam.DoFn):
  """PredictionResult -> dict row for BQ."""
  def __init__(self, top_k: int, model_name: str):
    self.top_k = top_k
    self.model_name = model_name

  def process(self, kv: Tuple[str, PredictionResult]):
    image_id, pred = kv
    logits = pred.inference[
        "logits"]  # torch.Tensor [B, num_classes] or [num_classes]
    if isinstance(logits, torch.Tensor) and logits.ndim == 1:
      logits = logits.unsqueeze(0)

    probs = F.softmax(logits, dim=-1)  # [B, C]
    values, indices = torch.topk(
        probs, k=min(self.top_k, probs.shape[-1]), dim=-1
    )

    topk = [{
        "class_id": int(idx.item()), "score": float(val.item())
    } for idx, val in zip(indices[0], values[0])]

    yield {
        "image_id": image_id,
        "model_name": self.model_name,
        "topk": json.dumps(topk),
        "infer_ms": now_millis(),
    }


# ============ Args & Helpers ============


def parse_known_args(argv):
  parser = argparse.ArgumentParser()
  # I/O & runtime
  parser.add_argument(
      '--mode', default='streaming', choices=['streaming', 'batch'])
  parser.add_argument(
      '--output_table',
      required=True,
      help='BigQuery output table: dataset.table')
  parser.add_argument(
      '--publish_to_big_query', default='true', choices=['true', 'false'])
  parser.add_argument(
      '--input_mode', default='gcs_uris', choices=['gcs_uris', 'bytes'])
  parser.add_argument(
      '--input',
      required=True,
      help='GCS path to file with URIs (for load) OR unused for bytes')
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
      default='efficientnet_b0',
      help='OSS model name (e.g., efficientnet_b0|mobilenetv3_large_100)')
  parser.add_argument(
      '--model_state_dict_path',
      default=None,
      help='Optional state_dict to load')
  parser.add_argument('--device', default='GPU', choices=['CPU', 'GPU'])
  parser.add_argument('--image_size', type=int, default=224)
  parser.add_argument('--top_k', type=int, default=5)
  parser.add_argument(
      '--inference_batch_size',
      default='auto',
      help='int or "auto"; auto tries 64→32→16')

  # Windows
  parser.add_argument('--window_sec', type=int, default=60)
  parser.add_argument('--trigger_proc_time_sec', type=int, default=30)

  # Dedup
  parser.add_argument(
      '--enable_dedup', default='false', choices=['true', 'false'])

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


# ============ Model factory (timm) ============


def create_timm_m(model_name: str, num_classes: int = 1000):
  import timm
  model = timm.create_model(
      model_name, pretrained=True, num_classes=num_classes)
  model.eval()
  return model


def pick_batch_size(arg: str) -> Optional[int]:
  if isinstance(arg, str) and arg.lower() == 'auto':
    return None
  try:
    return int(arg)
  except Exception:
    return None


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

  ensure_pubsub_resources(
      project=known_args.project,
      topic_path=known_args.pubsub_topic,
      subscription_path=known_args.pubsub_subscription)

  if known_args.mode == 'streaming':
    # Start feeder thread that reads URIs from GCS and fills Pub/Sub.
    threading.Thread(
        target=lambda:
        (time.sleep(900), run_load_pipeline(known_args, pipeline_args)),
        daemon=True).start()

  # StandardOptions
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = (
      known_args.mode == 'streaming')

  # Build model handler with right-fitting batch size
  desired_batch = pick_batch_size(known_args.inference_batch_size)
  tried = [64, 32, 16] if desired_batch is None else [desired_batch]

  # Device
  device = 'GPU' if known_args.device.upper() == 'GPU' else 'CPU'

  bs_ok = None
  last_err = None
  for bs in tried:
    try:
      model_handler = PytorchModelHandlerTensor(
          model_class=lambda: create_timm_m(known_args.pretrained_model_name),
          model_params={},
          state_dict_path=known_args.model_state_dict_path,
          device=device,
          inference_batch_size=bs
          if bs is not None else 64,  # start guess for warmup
      )
      # quick warmup to validate memory (single dummy tensor)
      dummy = torch.zeros((3, known_args.image_size, known_args.image_size),
                          dtype=torch.float32)
      _ = model_handler.load_model()  # ensures weights on device
      with torch.no_grad():
        mdl = model_handler._model
        mdl(torch.unsqueeze(dummy, 0))
      bs_ok = bs if bs is not None else 64
      break
    except Exception as e:
      last_err = e
      logging.warning("Batch size %s failed during warmup: %s", bs, e)
      continue

  if bs_ok is None:
    logging.warning(
        "Falling back to batch_size=8 due to previous errors: %s", last_err)
    bs_ok = 8
    model_handler = PytorchModelHandlerTensor(
        model_class=lambda: create_timm_m(known_args.pretrained_model_name),
        model_params={},
        state_dict_path=known_args.model_state_dict_path,
        device=device,
        inference_batch_size=bs_ok,
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

  keyed = (
      pcoll
      | 'MakeKey' >> beam.ParDo(MakeKeyDoFn(input_mode=known_args.input_mode)))

  if known_args.enable_dedup == 'true':
    keyed = keyed | 'Dedup' >> beam.ParDo(DedupDoFn())

  preprocessed = (
      keyed
      | 'DecodePreprocess' >> beam.ParDo(
          DecodePreprocessDoFn(
              input_mode=known_args.input_mode,
              image_size=known_args.image_size)))

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
              top_k=known_args.top_k,
              model_name=known_args.pretrained_model_name)))

  if known_args.publish_to_big_query == 'true':
    _ = (
        results
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema=
            'image_id:STRING, model_name:STRING, topk:STRING, infer_ms:INT64',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS))

  result = pipeline.run()
  result.wait_until_finish(duration=1800000)  # 30 min
  try:
    result.cancel()
  except Exception:
    pass

  cleanup_pubsub_resources(
      project=known_args.project,
      topic_path=known_args.pubsub_topic,
      subscription_path=known_args.pubsub_subscription)

  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
