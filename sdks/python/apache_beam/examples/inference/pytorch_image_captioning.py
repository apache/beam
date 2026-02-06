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

"""This pipeline performs image captioning using a multi-model approach:
BLIP generates candidate captions, CLIP ranks them by image-text similarity.

The pipeline reads image URIs from a GCS input file, decodes images, runs BLIP
caption generation in batches on GPU, then runs CLIP ranking in batches on GPU.
Results are written to BigQuery.
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
from typing import Tuple

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.runner import PipelineResult
from apache_beam.transforms import window

from google.cloud import pubsub_v1
import torch
import PIL.Image as PILImage

# ============ Utility ============


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


def sha1_hex(s: str) -> str:
  import hashlib
  return hashlib.sha1(s.encode("utf-8")).hexdigest()


def decode_pil(image_bytes: bytes) -> PILImage.Image:
  with PILImage.open(io.BytesIO(image_bytes)) as img:
    img = img.convert("RGB")
    img.load()
    return img


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


class ReadImageBytesDoFn(beam.DoFn):
  """Turn (image_id, uri) -> (image_id, dict(image_bytes, uri))."""
  def process(self, kv: Tuple[str, str]):
    image_id, uri = kv
    try:
      b = load_image_from_uri(uri)
      yield image_id, {"image_bytes": b, "uri": uri}
    except Exception as e:
      logging.warning("Failed to read image %s (%s): %s", image_id, uri, e)
      return


class PostProcessDoFn(beam.DoFn):
  """Final PredictionResult -> row for BigQuery."""
  def __init__(self, blip_name: str, clip_name: str):
    self.blip_name = blip_name
    self.clip_name = clip_name

  def process(self, kv: Tuple[str, PredictionResult]):
    image_id, pred = kv
    if hasattr(pred, "inference"):
      inf = pred.inference or {}
    else:
      inf = pred
    # Expected inference fields from CLIP handler:
    # best_caption, best_score, candidates, scores, blip_ms, clip_ms, total_ms
    best_caption = inf.get("best_caption", "")
    best_score = inf.get("best_score", None)
    candidates = inf.get("candidates", [])
    scores = inf.get("scores", [])
    blip_ms = inf.get("blip_ms", None)
    clip_ms = inf.get("clip_ms", None)
    total_ms = inf.get("total_ms", None)

    yield {
        "image_id": image_id,
        "blip_model": self.blip_name,
        "clip_model": self.clip_name,
        "best_caption": best_caption,
        "best_score": float(best_score) if best_score is not None else None,
        "candidates": json.dumps(candidates),
        "scores": json.dumps(scores),
        "blip_ms": int(blip_ms) if blip_ms is not None else None,
        "clip_ms": int(clip_ms) if clip_ms is not None else None,
        "total_ms": int(total_ms) if total_ms is not None else None,
        "infer_ms": now_millis(),
    }


# ============ Model Handlers ============


class BlipCaptionModelHandler(ModelHandler):
  def __init__(
      self,
      model_name: str,
      device: str,
      batch_size: int,
      num_captions: int,
      max_new_tokens: int,
      num_beams: int):
    self.model_name = model_name
    self.device = device
    self.batch_size = batch_size
    self.num_captions = num_captions
    self.max_new_tokens = max_new_tokens
    self.num_beams = num_beams

    self._model = None
    self._processor = None

  def load_model(self):
    from transformers import BlipForConditionalGeneration, BlipProcessor
    self._processor = BlipProcessor.from_pretrained(self.model_name)
    self._model = BlipForConditionalGeneration.from_pretrained(self.model_name)
    self._model.eval()
    self._model.to(self.device)
    return self._model

  def batch_elements_kwargs(self):
    return {"max_batch_size": self.batch_size}

  def run_inference(
      self, batch: List[Dict[str, Any]], model, inference_args=None):

    if model is not None:
      self._model = model
      self._model.to(self.device)
      self._model.eval()
    if self._processor is None:
      from transformers import BlipProcessor
      self._processor = BlipProcessor.from_pretrained(self.model_name)
    if self._model is None:
      self._model = self.load_model()

    start = now_millis()

    images = []
    uris = []
    bytes_list = []
    for x in batch:
      b = x["image_bytes"]
      bytes_list.append(b)
      uris.append(x.get("uri", ""))
      try:
        images.append(decode_pil(b))
      except Exception:
        # fallback: a blank image (so pipeline keeps going)
        images.append(PILImage.new("RGB", (224, 224), color=(0, 0, 0)))

    # Processor makes pixel_values
    inputs = self._processor(images=images, return_tensors="pt")
    pixel_values = inputs["pixel_values"].to(self.device)

    # Generate captions
    # We use num_return_sequences to generate multiple candidates per image.
    # Note: this will produce (B * num_captions) sequences.
    with torch.no_grad():
      generated_ids = self._model.generate(
          pixel_values=pixel_values,
          max_new_tokens=self.max_new_tokens,
          num_beams=max(self.num_beams, self.num_captions),
          num_return_sequences=self.num_captions,
          do_sample=False,
      )

    captions_all = self._processor.batch_decode(
        generated_ids, skip_special_tokens=True)

    # Group candidates per image
    candidates_per_image = []
    idx = 0
    for _ in range(len(batch)):
      candidates_per_image.append(captions_all[idx:idx + self.num_captions])
      idx += self.num_captions

    blip_ms = now_millis() - start

    results = []
    for i in range(len(batch)):
      results.append({
          "image_bytes": bytes_list[i],
          "uri": uris[i],
          "candidates": candidates_per_image[i],
          "blip_ms": blip_ms,
      })
    return results

  def get_metrics_namespace(self) -> str:
    return "blip_captioning"


class ClipRankModelHandler(ModelHandler):
  def __init__(
      self,
      model_name: str,
      device: str,
      batch_size: int,
      score_normalize: bool):
    self.model_name = model_name
    self.device = device
    self.batch_size = batch_size
    self.score_normalize = score_normalize

    self._model = None
    self._processor = None

  def load_model(self):
    from transformers import CLIPModel, CLIPProcessor
    self._processor = CLIPProcessor.from_pretrained(self.model_name)
    self._model = CLIPModel.from_pretrained(self.model_name)
    self._model.eval()
    self._model.to(self.device)
    return self._model

  def batch_elements_kwargs(self):
    return {"max_batch_size": self.batch_size}

  def run_inference(
      self, batch: List[Dict[str, Any]], model, inference_args=None):

    if model is not None:
      self._model = model
      self._model.to(self.device)
      self._model.eval()
    if self._processor is None:
      from transformers import CLIPProcessor
      self._processor = CLIPProcessor.from_pretrained(self.model_name)
    if self._model is None:
      self._model = self.load_model()

    start_batch = now_millis()

    # Flat lists for a single batched CLIP forward pass
    images: List[PILImage.Image] = []
    texts: List[str] = []
    offsets: List[Tuple[int, int]] = []
    # per element -> [start, end) in flat arrays
    candidates_list: List[List[str]] = []
    blip_ms_list: List[Optional[int]] = []

    for x in batch:
      image_bytes = x["image_bytes"]
      candidates = [str(c) for c in (x.get("candidates", []) or [])]
      candidates_list.append(candidates)
      blip_ms_list.append(x.get("blip_ms", None))

      try:
        img = decode_pil(image_bytes)
      except Exception:
        img = PILImage.new("RGB", (224, 224), color=(0, 0, 0))

      start_i = len(texts)
      for c in candidates:
        images.append(img)
        texts.append(c)
      end_i = len(texts)
      offsets.append((start_i, end_i))

    results: List[Dict[str, Any]] = []

    # Fast path: no candidates at all
    if not texts:
      for blip_ms in blip_ms_list:
        total_ms = int(blip_ms) if blip_ms is not None else None
        results.append({
            "best_caption": "",
            "best_score": None,
            "candidates": [],
            "scores": [],
            "blip_ms": blip_ms,
            "clip_ms": 0,
            "total_ms": total_ms,
        })
      return results

    with torch.no_grad():
      inputs = self._processor(
          text=texts,
          images=images,
          return_tensors="pt",
          padding=True,
          truncation=True,
      )
      inputs = {
          k: (v.to(self.device) if torch.is_tensor(v) else v)
          for k, v in inputs.items()
      }

      # avoid NxN logits inside CLIPModel.forward()
      img = self._model.get_image_features(
          pixel_values=inputs["pixel_values"])  # [N, D]
      txt = self._model.get_text_features(
          input_ids=inputs["input_ids"],
          attention_mask=inputs.get("attention_mask"),
      )  # [N, D]

      img = img / img.norm(dim=-1, keepdim=True)
      txt = txt / txt.norm(dim=-1, keepdim=True)

      logit_scale = self._model.logit_scale.exp()  # scalar tensor
      pair_scores = (img * txt).sum(dim=-1) * logit_scale  # [N]
      pair_scores_cpu = pair_scores.detach().cpu().tolist()

    batch_ms = now_millis() - start_batch
    total_pairs = len(texts)

    items = zip(offsets, candidates_list, blip_ms_list)
    for (start_i, end_i), candidates, blip_ms in items:
      if start_i == end_i:
        total_ms = int(blip_ms) if blip_ms is not None else None
        results.append({
            "best_caption": "",
            "best_score": None,
            "candidates": [],
            "scores": [],
            "blip_ms": blip_ms,
            "clip_ms": 0,
            "total_ms": total_ms,
        })
        continue

      scores = [float(pair_scores_cpu[j]) for j in range(start_i, end_i)]

      if self.score_normalize:
        scores_t = torch.tensor(scores, dtype=torch.float32)
        scores = torch.softmax(scores_t, dim=0).tolist()

      best_idx = max(range(len(scores)), key=lambda i, s=scores: s[i])

      pairs = end_i - start_i
      clip_ms_elem = int(batch_ms * (pairs / max(1, total_pairs)))
      if pairs > 0:
        clip_ms_elem = max(1, clip_ms_elem)

      total_ms = int(blip_ms) + clip_ms_elem if blip_ms is not None else None
      results.append({
          "best_caption": candidates[best_idx],
          "best_score": float(scores[best_idx]),
          "candidates": candidates,
          "scores": scores,
          "blip_ms": blip_ms,
          "clip_ms": clip_ms_elem,
          "total_ms": total_ms,
      })

    return results

  def get_metrics_namespace(self) -> str:
    return "clip_ranking"


# ============ Args & Helpers ============


def parse_known_args(argv):
  parser = argparse.ArgumentParser()

  # I/O & runtime
  parser.add_argument(
      '--mode', default='streaming', choices=['streaming', 'batch'])
  parser.add_argument(
      '--project', default='apache-beam-testing', help='GCP project ID')
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
  parser.add_argument(
      '--output_table',
      required=True,
      help='BigQuery output table: dataset.table')
  parser.add_argument(
      '--publish_to_big_query', default='true', choices=['true', 'false'])

  # Device
  parser.add_argument('--device', default='GPU', choices=['CPU', 'GPU'])

  # BLIP
  parser.add_argument(
      '--blip_model_name', default='Salesforce/blip-image-captioning-base')
  parser.add_argument('--blip_batch_size', type=int, default=4)
  parser.add_argument('--num_captions', type=int, default=5)
  parser.add_argument('--max_new_tokens', type=int, default=30)
  parser.add_argument('--num_beams', type=int, default=5)

  # CLIP
  parser.add_argument(
      '--clip_model_name', default='openai/clip-vit-base-patch32')
  parser.add_argument('--clip_batch_size', type=int, default=8)
  parser.add_argument(
      '--clip_score_normalize', default='false', choices=['true', 'false'])

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

  device = 'cuda' if known_args.device.upper() == 'GPU' else 'cpu'
  clip_score_normalize = (known_args.clip_score_normalize == 'true')

  blip_handler = BlipCaptionModelHandler(
      model_name=known_args.blip_model_name,
      device=device,
      batch_size=int(known_args.blip_batch_size),
      num_captions=int(known_args.num_captions),
      max_new_tokens=int(known_args.max_new_tokens),
      num_beams=int(known_args.num_beams),
  )

  clip_handler = ClipRankModelHandler(
      model_name=known_args.clip_model_name,
      device=device,
      batch_size=int(known_args.clip_batch_size),
      score_normalize=clip_score_normalize,
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

  images = (keyed | 'ReadImageBytes' >> beam.ParDo(ReadImageBytesDoFn()))

  # Stage 1: BLIP candidate generation
  blip_out = (
      images
      | 'RunInferenceBLIP' >> RunInference(KeyedModelHandler(blip_handler)))

  # Stage 2: CLIP ranking over candidates
  clip_out = (
      blip_out
      | 'RunInferenceCLIP' >> RunInference(KeyedModelHandler(clip_handler)))

  results = (
      clip_out
      | 'PostProcess' >> beam.ParDo(
          PostProcessDoFn(
              blip_name=known_args.blip_model_name,
              clip_name=known_args.clip_model_name)))

  method = (
      beam.io.WriteToBigQuery.Method.FILE_LOADS if known_args.mode == 'batch'
      else beam.io.WriteToBigQuery.Method.STREAMING_INSERTS)

  if known_args.publish_to_big_query == 'true':
    _ = (
        results
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema=(
                'image_id:STRING, blip_model:STRING, clip_model:STRING, '
                'best_caption:STRING, best_score:FLOAT, '
                'candidates:STRING, scores:STRING, '
                'blip_ms:INT64, clip_ms:INT64, total_ms:INT64, infer_ms:INT64'),
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
