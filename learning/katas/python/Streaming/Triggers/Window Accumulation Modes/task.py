#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# beam-playground:
#   name: WindowAccumulationMode
#   description: Task from katas to count events using ACCUMULATING as accumulation mode
#   multifile: true
#   files:
#    - name: generate_event.py
#   context_line: 60
#   categories:
#     - Streaming
#   complexity: ADVANCED
#   tags:
#     - windowing
#     - triggers
#     - count
#     - accumulation
#     - event

import apache_beam as beam
from generate_event import GenerateEvent
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.utils.timestamp import Duration
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.util import LogElements


class CountEventsWithAccumulating(beam.PTransform):
  def expand(self, events):
    return (events
            | beam.WindowInto(FixedWindows(1 * 24 * 60 * 60),  # 1 Day Window
                              trigger=AfterWatermark(early=AfterCount(1)),
                              accumulation_mode=AccumulationMode.ACCUMULATING,
                              allowed_lateness=Duration(seconds=0))
            | beam.CombineGlobally(beam.combiners.CountCombineFn()).without_defaults())


options = PipelineOptions()
options.view_as(StandardOptions).streaming = True
with beam.Pipeline(options=options) as p:
  (p | GenerateEvent.sample_data()
   | CountEventsWithAccumulating()
   | LogElements(with_window=True))
