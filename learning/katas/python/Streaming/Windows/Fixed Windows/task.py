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
#   name: WindowingFixedTime
#   description: Task from katas to count the number of events that happened based on fixed window with 1-day duration.
#   multifile: false
#   context_line: 39
#   categories:
#     - Windowing
#     - Combiners
#   complexity: MEDIUM
#   tags:
#     - windowing
#     - combine
#     - count
#     - event

from datetime import datetime
import pytz

import apache_beam as beam
from apache_beam.transforms import window


with beam.Pipeline() as p:

  (p | beam.Create([
          window.TimestampedValue("event", datetime(2020, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 5, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 5, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 8, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 8, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 8, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
          window.TimestampedValue("event", datetime(2020, 3, 10, 0, 0, 0, 0, tzinfo=pytz.UTC).timestamp()),
       ])
     | beam.WindowInto(window.FixedWindows(24*60*60))
     | beam.combiners.Count.PerElement()
     | beam.LogElements(with_window=True))
