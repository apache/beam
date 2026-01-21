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

"""This batch pipeline performs image captioning using a multi-model approach:
BLIP generates candidate captions, CLIP ranks them by image-text similarity.

The pipeline reads image URIs from a GCS input file, decodes images, runs BLIP
caption generation in batches on GPU, then runs CLIP ranking in batches on GPU.
Results are written to BigQuery using FILE_LOADS for stable batch semantics.

Exactly-once semantics for batch runs are approximated via a stable image_id
(sha1(uri)) + Distinct() before writing and FILE_LOADS output method.
"""

import argparse
import io
import json
import logging
import time
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
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
    return img.convert("RGB")


# ============ DoFns ============


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
    inf = pred.inference or {}
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
    if self._model is None:
      self.load_model()

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
    if self._model is None:
      self.load_model()

    start = now_millis()

    results = []
    with torch.no_grad():
      for x in batch:
        image_bytes = x["image_bytes"]
        candidates = x.get("candidates", [])
        blip_ms = x.get("blip_ms", None)

        # Decode image
        try:
          image = decode_pil(image_bytes)
        except Exception:
          image = PILImage.new("RGB", (224, 224), color=(0, 0, 0))

        if not candidates:
          clip_ms = now_millis() - start
          results.append({
              "best_caption": "",
              "best_score": None,
              "candidates": [],
              "scores": [],
              "blip_ms": blip_ms,
              "clip_ms": clip_ms,
              "total_ms": None,
          })
          continue

        # CLIPProcessor can accept a single image and list of texts
        inputs = self._processor(
            text=candidates, images=image, return_tensors="pt", padding=True)
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        outputs = self._model(**inputs)
        # logits_per_image shape: [1, num_texts]
        logits = outputs.logits_per_image[0]

        if self.score_normalize:
          # optional normalization to [0..1] via softmax
          probs = torch.softmax(logits, dim=-1)
          scores_t = probs
        else:
          scores_t = logits

        scores = scores_t.detach().cpu().tolist()
        best_idx = int(torch.argmax(scores_t).item())
        best_caption = candidates[best_idx]
        best_score = float(scores[best_idx])

        clip_ms = now_millis() - start
        total_ms = None
        if blip_ms is not None:
          total_ms = int(blip_ms) + int(clip_ms)

        results.append({
            "best_caption": best_caption,
            "best_score": best_score,
            "candidates": candidates,
            "scores": scores,
            "blip_ms": blip_ms,
            "clip_ms": clip_ms,
            "total_ms": total_ms,
        })

    return results

  def get_metrics_namespace(self) -> str:
    return "clip_ranking"


# ============ Args & Helpers ============


def parse_known_args(argv):
  parser = argparse.ArgumentParser()

  # I/O & runtime
  parser.add_argument('--mode', default='batch', choices=['batch'])
  parser.add_argument(
      '--project', default='apache-beam-testing', help='GCP project ID')
  parser.add_argument(
      '--input', required=True, help='GCS path to file with image URIs')
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

  known_args, pipeline_args = parser.parse_known_args(argv)
  return known_args, pipeline_args


# ============ Main pipeline ============


def run(
    argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
  known_args, pipeline_args = parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = False

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

  pcoll = (
      pipeline
      | 'ReadURIsBatch' >> beam.Create(
          list(read_gcs_file_lines(known_args.input)))
      | 'FilterEmptyBatch' >> beam.Filter(lambda s: s.strip()))

  keyed = (
      pcoll
      | 'MakeKey' >> beam.ParDo(MakeKeyDoFn())
      | 'DistinctByKey' >> beam.Distinct())

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
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS))

  result = pipeline.run()
  result.wait_until_finish(duration=1800000)  # 30 min
  try:
    result.cancel()
  except Exception:
    pass
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
