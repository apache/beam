#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Utilities to be used in  Interactive Beam.
"""

from __future__ import absolute_import

import pandas as pd

from apache_beam.portability.api.beam_runner_api_pb2 import TestStreamPayload


def to_element_list(
    reader, # type: Generator[Union[TestStreamPayload.Event, WindowedValueHolder]]
    coder, # type: Coder
    include_window_info # type: bool
    ):
  # type: (...) -> List[WindowedValue]

  """Returns an iterator that properly decodes the elements from the reader.
  """

  for e in reader:
    if isinstance(e, TestStreamPayload.Event):
      if (e.HasField('watermark_event') or e.HasField('processing_time_event')):
        continue
      else:
        for tv in e.element_event.elements:
          decoded = coder.decode(tv.encoded_element)
          yield (
              decoded.windowed_value
              if include_window_info else decoded.windowed_value.value)
    else:
      yield e.windowed_value if include_window_info else e.windowed_value.value


def elements_to_df(elements, include_window_info=False):
  # type: (List[WindowedValue], bool) -> DataFrame

  """Parses the given elements into a Dataframe.

  If the elements are a list of WindowedValues, then it will break out the
  elements into their own DataFrame and return it. If include_window_info is
  True, then it will concatenate the windowing information onto the elements
  DataFrame.
  """

  rows = []
  windowed_info = []
  for e in elements:
    rows.append(e.value)
    if include_window_info:
      windowed_info.append([e.timestamp.micros, e.windows, e.pane_info])

  rows_df = pd.DataFrame(rows)
  if include_window_info:
    windowed_info_df = pd.DataFrame(
        windowed_info, columns=['event_time', 'windows', 'pane_info'])
    final_df = pd.concat([rows_df, windowed_info_df], axis=1)
  else:
    final_df = rows_df

  return final_df
