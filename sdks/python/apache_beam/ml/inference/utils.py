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

"""
Util/helper functions used in apache_beam.ml.inference.
"""

import os
from functools import partial
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Union

import apache_beam as beam
from apache_beam.io.fileio import MatchContinuously
from apache_beam.ml.inference.base import ModelMetdata
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.transforms import window
from apache_beam.transforms import trigger
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

_START_TIME_STAMP = Timestamp.now()


def _convert_to_result(
    batch: Iterable,
    predictions: Union[Iterable, Dict[Any, Iterable]],
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
        PredictionResult(x, y, model_id) for x,
        y in zip(batch, predictions_per_tensor)
    ]
  return [PredictionResult(x, y, model_id) for x, y in zip(batch, predictions)]


class _CoverIterToSingleton(beam.DoFn):
  """
  Internal only; No backwards compatibility.

  The MatchContinuously transform examines all files present in a given
  directory and returns those that have timestamps older than the
  pipeline's start time. This can produce an Iterable rather than a
  Singleton. This class only returns the file path when it is first
  encountered, and it is cached as part of the side input caching mechanism.
  If the path is seen again, it will not return anything.
  By doing this, we can ensure that the output of this transform can be wrapped
  with beam.pvalue.AsSingleton().
  """
  COUNT_STATE = CombiningValueStateSpec('count', combine_fn=sum)

  def process(self, element, count_state=beam.DoFn.StateParam(COUNT_STATE)):
    counter = count_state.read()
    if counter == 0:
      count_state.add(1)
      yield element[1]


class _GetLatestFileByTimeStamp(beam.DoFn):
  """
  Internal only; No backwards compatibility.

  This DoFn checks the timestamps of files against the time that the pipeline
  began running. It returns the files that were modified after the pipeline
  started. If no such files are found, it returns a default file as fallback.

  """
  TIME_STATE = CombiningValueStateSpec(
      'count', combine_fn=partial(max, default=_START_TIME_STAMP))

  def __init__(self, default_value):
    self._default_value = default_value

  def process(
      self, element, time_state=beam.DoFn.StateParam(TIME_STATE)
  ) -> List[Tuple[str, ModelMetdata]]:
    path, file_metadata = element
    new_ts = file_metadata.last_updated_in_seconds
    old_ts = time_state.read()
    if new_ts > old_ts:
      time_state.clear()
      time_state.add(new_ts)
      return [(
          path,
          ModelMetdata(
              model_id=file_metadata.path,
              model_name=os.path.splitext(os.path.basename(
                  file_metadata.path))[0]))]
    else:
      path_short_name = None
      if self._default_value:
        path_short_name = os.path.splitext(
            os.path.basename(self._default_value))[0]
      return [(
          self._default_value,
          ModelMetdata(
              model_id=self._default_value, model_name=path_short_name))]


def _convert_to_result(
    batch: Iterable,
    predictions: Union[Iterable, Dict[Any, Iterable]],
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
        PredictionResult(x, y, model_id) for x,
        y in zip(batch, predictions_per_tensor)
    ]
  return [PredictionResult(x, y, model_id) for x, y in zip(batch, predictions)]


class WatchFilePattern(beam.PTransform):
  def __init__(
      self,
      file_pattern,
      interval=360,
      stop_timestamp=MAX_TIMESTAMP,
      default_value=None,
  ):
    """
    Watch for updates using the file pattern using MatchContinuously transform.

    Note: Start timestamp will be defaulted to timestamp when pipeline was run.
      All the files matching file_pattern, that are uploaded before the
      pipeline started will be discarded.

      Args:
        file_pattern: The file path to read from.
        interval: Interval at which to check for files in seconds.
        stop_timestamp: Timestamp after which no more files will be checked.
    """
    self.file_pattern = file_pattern
    self.interval = interval
    self.stop_timestamp = stop_timestamp
    self._latest_timestamp = None
    self._default_value = default_value

  def expand(self, pcoll) -> beam.PCollection[ModelMetdata]:
    return (
        pcoll
        | 'MatchContinuously' >> MatchContinuously(
            file_pattern=self.file_pattern,
            interval=self.interval,
            stop_timestamp=self.stop_timestamp)
        | "AttachKey" >> beam.Map(lambda x: (x.path, x))
        | "GetLatestFileMetaData" >> beam.ParDo(
            _GetLatestFileByTimeStamp(default_value=self._default_value))
        | "AcceptNewSideInputOnly" >> beam.ParDo(_CoverIterToSingleton())
        | 'ApplyGlobalWindow' >> beam.transforms.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(
                trigger.AfterProcessingTime(self.interval)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING))
