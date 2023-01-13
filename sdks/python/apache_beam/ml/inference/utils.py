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

import apache_beam as beam
from apache_beam.io.fileio import MatchContinuously
from apache_beam.ml.inference.base import ModelMetdata
from apache_beam.transforms import window
from apache_beam.transforms import trigger
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp


class WatchFilePattern(beam.PTransform):
  def __init__(
      self,
      file_pattern,
      interval=360,
      start_timestamp=Timestamp.now(),
      stop_timestamp=MAX_TIMESTAMP,
      match_updated_files=False,
      has_deduplication=True):
    """
    Watch for updates using the file pattern using MatchContinuously transform.

      Args:
        file_pattern: The file path to read from.
        interval: Interval at which to check for files in seconds.
        has_deduplication: Whether files already read are discarded or not.
        start_timestamp: Timestamp for start file checking.
        stop_timestamp: Timestamp after which no more files will be checked.
        match_updated_files: (When has_deduplication is set to True) whether
          match file with timestamp changes.
    """
    self.file_pattern = file_pattern
    self.interval = interval
    self.start_timestamp = start_timestamp
    self.stop_timestamp = stop_timestamp
    self.match_updated_files = match_updated_files
    self.has_deduplication = has_deduplication

  def expand(self, pcoll) -> beam.PCollection[ModelMetdata]:
    return (
        pcoll
        | 'MatchContinuously' >> MatchContinuously(
            file_pattern=self.file_pattern,
            interval=self.interval,
            start_timestamp=self.start_timestamp,
            stop_timestamp=self.stop_timestamp,
            match_updated_files=self.match_updated_files,
            has_deduplication=self.has_deduplication)
        | 'ApplyGlobalWindow' >> beam.transforms.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(
                trigger.AfterProcessingTime(self.interval)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | "GetModelPath" >> beam.Map(lambda x: ModelMetdata(model_id=x.path)))
