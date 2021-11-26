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

import apache_beam as beam
from apache_beam.testing.test_stream import TestStream
from datetime import datetime
import pytz


class GenerateEvent(beam.PTransform):

    @staticmethod
    def sample_data():
        return GenerateEvent()

    def expand(self, input):

        return (input
                | TestStream()
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 1, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 2, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 3, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 4, 0, tzinfo=pytz.UTC).timestamp())
                    .advance_watermark_to(datetime(2021, 3, 1, 0, 0, 5, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 5, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 6, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 7, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 8, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 9, 0, tzinfo=pytz.UTC).timestamp())
                    .advance_watermark_to(datetime(2021, 3, 1, 0, 0, 10, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 10, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 11, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 12, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 13, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 14, 0, tzinfo=pytz.UTC).timestamp())
                    .advance_watermark_to(datetime(2021, 3, 1, 0, 0, 15, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 15, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 16, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 17, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 18, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 19, 0, tzinfo=pytz.UTC).timestamp())
                    .advance_watermark_to(datetime(2021, 3, 1, 0, 0, 20, 0, tzinfo=pytz.UTC).timestamp())
                    .add_elements(elements=["event"], event_timestamp=datetime(2021, 3, 1, 0, 0, 20, 0, tzinfo=pytz.UTC).timestamp())
                    .advance_watermark_to(datetime(2021, 3, 1, 0, 0, 25, 0, tzinfo=pytz.UTC).timestamp())
                    .advance_watermark_to_infinity())
