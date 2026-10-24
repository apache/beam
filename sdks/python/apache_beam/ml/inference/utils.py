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
# pytype: skip-file

"""
Util/helper functions used in apache_beam.ml.inference.
"""
import os
from collections.abc import Iterable
from typing import Any
from typing import Optional
from typing import Union

import apache_beam as beam
from apache_beam.coders import FloatCoder
from apache_beam.io.fileio import EmptyMatchTreatment
from apache_beam.io.fileio import MatchContinuously
from apache_beam.io.filesystem import FileMetadata
from apache_beam.ml.inference.base import ModelMetadata
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

_START_TIME_STAMP = Timestamp.now()

# Beam keeps DoFn state per key. Keying each file by its own path gives every
# path a fresh timestamp and never compares models with each other, so every
# watched file is keyed the same way.
_GLOBAL_MODEL_KEY = 'WatchFilePattern'


def _convert_to_result(
    batch: Iterable,
    predictions: Union[Iterable, dict[Any, Iterable]],
    model_id: Optional[str] = None,
) -> Iterable[PredictionResult]:
  if isinstance(predictions, dict):
    # Go from one dictionary of type: {key_type1: Iterable<val_type1>,
    # key_type2: Iterable<val_type2>, ...} where each Iterable is of
    # length batch_size, to a list of dictionaries:
    # [{key_type1: value_type1, key_type2: value_type2}]
    predictions_per_tensor = [
        dict(zip(predictions.keys(), v)) for v in zip(*predictions.values())
    ]
    return [
        PredictionResult(x, y, model_id)
        for x, y in zip(batch, predictions_per_tensor)
    ]
  return [PredictionResult(x, y, model_id) for x, y in zip(batch, predictions)]


def _as_seconds(timestamp: Union[Timestamp, float]) -> float:
  return float(timestamp)


def _is_newer(
    candidate: Optional[FileMetadata],
    incumbent: Optional[FileMetadata]) -> bool:
  """Return true when candidate should replace incumbent as the latest file.

  The newest modification time wins. Equal timestamps are broken by path so
  the choice is the same no matter which order the files are merged in.
  """
  if candidate is None:
    return False
  if incumbent is None:
    return True
  candidate_ts = _as_seconds(candidate.last_updated_in_seconds)
  incumbent_ts = _as_seconds(incumbent.last_updated_in_seconds)
  if candidate_ts != incumbent_ts:
    return candidate_ts > incumbent_ts
  return candidate.path > incumbent.path


def _with_global_key(metadata: FileMetadata):
  return _GLOBAL_MODEL_KEY, metadata


def _model_metadata(path: str) -> ModelMetadata:
  model_name = os.path.splitext(os.path.basename(path))[0]
  return ModelMetadata(model_id=path, model_name=model_name)


class _LatestFileCombineFn(beam.CombineFn):
  """Selects the FileMetadata with the newest modification time.

  Internal only; no backwards compatibility.
  """
  def create_accumulator(self):
    return None

  def add_input(self, accumulator, element):
    if _is_newer(element, accumulator):
      return element
    return accumulator

  def merge_accumulators(self, accumulators):
    merged = None
    for accumulator in accumulators:
      if _is_newer(accumulator, merged):
        merged = accumulator
    return merged

  def extract_output(self, accumulator):
    return accumulator


class _EmitLatestModel(beam.DoFn):
  """Emits a model only when it is newer than every model seen so far.

  Internal only; no backwards compatibility.

  Input is ``(key, FileMetadata)`` and every file shares one key, so this
  state is the newest timestamp across the whole watch, not per path. Files
  at or before pipeline construction are the initial model and are not
  emitted. Emitting one value per pane keeps the output usable as a
  singleton side input.
  """
  LATEST_TS = ReadModifyWriteStateSpec('latest_ts', FloatCoder())

  def process(self, element, latest_ts=beam.DoFn.StateParam(LATEST_TS)):
    _, file_metadata = element
    if file_metadata is None:
      return
    new_ts = _as_seconds(file_metadata.last_updated_in_seconds)
    old_ts = latest_ts.read()
    if old_ts is None:
      old_ts = _as_seconds(_START_TIME_STAMP)
    if new_ts <= old_ts:
      return
    latest_ts.write(new_ts)
    yield _model_metadata(file_metadata.path)


class _SelectLatestModel(beam.PTransform):
  """Reduce watched files to singleton updates of the newest model.

  Internal only; no backwards compatibility.
  """
  def expand(self, pcoll) -> beam.PCollection[ModelMetadata]:
    # Window first so one poll's files are reduced together. RunInference
    # reads this output with AsSingleton, which rejects a pane that contains
    # more than one model. The timestamp state above then drops a later file
    # whose modification time is older than a model already emitted.
    return (
        pcoll
        | 'AttachKey' >> beam.Map(_with_global_key)
        | 'ApplyGlobalWindow' >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterProcessingTime(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | 'LatestFilePerPane' >> beam.CombinePerKey(_LatestFileCombineFn())
        | 'EmitLatestModel' >> beam.ParDo(_EmitLatestModel()))


class WatchFilePattern(beam.PTransform):
  def __init__(
      self,
      file_pattern,
      interval=360,
      stop_timestamp=MAX_TIMESTAMP,
  ):
    """Watches a directory for updates to files matching a given file pattern.

    Emits the matching file with the newest modification time, one model per
    pane, so the result can be read with
    :class:`~apache_beam.pvalue.AsSingleton`.

    Args:
      file_pattern: The file path to read from, as a local path or a GCS
        ``gs://`` path. The path can contain glob characters (``*``, ``?``,
        and ``[...]`` sets).
      interval: Interval, in seconds, at which to check for files matching
        ``file_pattern``.
      stop_timestamp: Timestamp after which no more files will be checked.

    **Note**:

    1. Each model update must use a new filename. A path is emitted only once,
       and ``RunInference`` reloads a model only when that path changes. To
       roll back, upload the desired model under a new name whose modification
       time is newer than the model currently in use. A copy that keeps an
       older timestamp (for example ``cp -p``) is not selected. On GCS the
       timestamp is the object upload time.
    2. A poll may observe several matching files. Only the file with the
       newest modification time is emitted.
    3. At least one file matching ``file_pattern`` must exist before the
       pipeline starts. Files whose modification time is at or before pipeline
       construction are not emitted. The model configured on the
       ``ModelHandler`` is used until a newer file appears.
    4. This transform is supported in streaming mode.
       ``MatchContinuously`` is an unbounded source, and running it in batch
       mode can stall the pipeline or drop updates.
    """
    self.file_pattern = file_pattern
    self.interval = interval
    self.stop_timestamp = stop_timestamp

  def expand(self, pcoll) -> beam.PCollection[ModelMetadata]:
    return (
        pcoll
        | 'MatchContinuously' >> MatchContinuously(
            file_pattern=self.file_pattern,
            interval=self.interval,
            stop_timestamp=self.stop_timestamp,
            empty_match_treatment=EmptyMatchTreatment.DISALLOW)
        | 'SelectLatestModel' >> _SelectLatestModel())
