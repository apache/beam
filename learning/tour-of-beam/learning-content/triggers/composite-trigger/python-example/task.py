#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# beam-playground:
#   name: composite-trigger
#   description: Composite trigger example.
#   multifile: false
#   context_line: 52
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

import apache_beam as beam
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms import trigger


# Output PCollection
class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):
        def __init__(self, prefix=''):
            super().__init__()
            self.prefix = prefix

        def process(self, element):
            print(self.prefix+str(element))

    def __init__(self, label=None,prefix=''):
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._OutputFn(self.prefix))

with beam.Pipeline() as p:
  processing_time_trigger = trigger.AfterProcessingTime(60)
# Define an event time trigger
  event_time_trigger = trigger.AfterWatermark(early=trigger.AfterCount(100),
                                             late=trigger.AfterCount(200))

# Combine the processing time and event time triggers using the Or method
  composite_trigger = trigger.AfterAll(processing_time_trigger,event_time_trigger)

  (p | beam.Create(['Hello Beam','It`s trigger'])
     | 'window' >>  beam.WindowInto(FixedWindows(2),
                                                trigger=composite_trigger ,
                                                accumulation_mode=trigger.AccumulationMode.DISCARDING)
     | 'Log words' >> Output())