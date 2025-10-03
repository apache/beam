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

from datetime import datetime

import pytz

import apache_beam as beam
from apache_beam.testing.test_stream import TestStream


class GenerateEvent(beam.PTransform):
  # pylint: disable=line-too-long
  """This class simulates streaming data.
  It leverages [TestStream](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/testing/TestStream.html),
  a method where you can control when data arrives and how watermark advances.
  This is especially useful in unit tests.""" # noqa

  @staticmethod
  def sample_data():
    return GenerateEvent()

  def expand(self, input):
    # these are the elements that will arrive in the simulated TestStream
    # at multiple timestamps
    elem = [{'age': 10}, {'age': 20}, {'age': 30}]

    # The simulated TestStream adds elements at specific timestamps
    # using add_elements and advances the watermark after 1 or more
    # elements are arrive using advance_watermark_to
    return (
        input
        | TestStream().add_elements(
            elements=elem,
            event_timestamp=datetime(
                2021, 3, 1, 0, 0, 1, 0,
                tzinfo=pytz.UTC).timestamp()).add_elements(
                    elements=elem,
                    event_timestamp=datetime(
                        2021, 3, 1, 0, 0, 2, 0,
                        tzinfo=pytz.UTC).timestamp()).add_elements(
                            elements=elem,
                            event_timestamp=datetime(
                                2021, 3, 1, 0, 0, 3, 0,
                                tzinfo=pytz.UTC).timestamp()).add_elements(
                                    elements=elem,
                                    event_timestamp=datetime(
                                        2021, 3, 1, 0, 0, 4, 0,
                                        tzinfo=pytz.UTC).timestamp()).
        advance_watermark_to(
            datetime(2021, 3, 1, 0, 0, 5, 0,
                     tzinfo=pytz.UTC).timestamp()).add_elements(
                         elements=elem,
                         event_timestamp=datetime(
                             2021, 3, 1, 0, 0, 5, 0,
                             tzinfo=pytz.UTC).timestamp()).
        add_elements(
            elements=elem,
            event_timestamp=datetime(
                2021, 3, 1, 0, 0, 6,
                0, tzinfo=pytz.UTC).timestamp()).add_elements(
                    elements=elem,
                    event_timestamp=datetime(
                        2021, 3, 1, 0, 0, 7, 0,
                        tzinfo=pytz.UTC).timestamp()).add_elements(
                            elements=elem,
                            event_timestamp=datetime(
                                2021, 3, 1, 0, 0, 8, 0,
                                tzinfo=pytz.UTC).timestamp()).add_elements(
                                    elements=elem,
                                    event_timestamp=datetime(
                                        2021, 3, 1, 0, 0, 9, 0,
                                        tzinfo=pytz.UTC).timestamp()).
        advance_watermark_to(
            datetime(2021, 3, 1, 0, 0, 10, 0,
                     tzinfo=pytz.UTC).timestamp()).add_elements(
                         elements=elem,
                         event_timestamp=datetime(
                             2021, 3, 1, 0, 0, 10, 0,
                             tzinfo=pytz.UTC).timestamp()).add_elements(
                                 elements=elem,
                                 event_timestamp=datetime(
                                     2021, 3, 1, 0, 0, 11, 0,
                                     tzinfo=pytz.UTC).timestamp()).
        add_elements(
            elements=elem,
            event_timestamp=datetime(
                2021, 3, 1, 0, 0, 12, 0,
                tzinfo=pytz.UTC).timestamp()).add_elements(
                    elements=elem,
                    event_timestamp=datetime(
                        2021, 3, 1, 0, 0, 13, 0,
                        tzinfo=pytz.UTC).timestamp()).add_elements(
                            elements=elem,
                            event_timestamp=datetime(
                                2021, 3, 1, 0, 0, 14, 0,
                                tzinfo=pytz.UTC).timestamp()).
        advance_watermark_to(
            datetime(2021, 3, 1, 0, 0, 15, 0,
                     tzinfo=pytz.UTC).timestamp()).add_elements(
                         elements=elem,
                         event_timestamp=datetime(
                             2021, 3, 1, 0, 0, 15, 0,
                             tzinfo=pytz.UTC).timestamp()).add_elements(
                                 elements=elem,
                                 event_timestamp=datetime(
                                     2021, 3, 1, 0, 0, 16, 0,
                                     tzinfo=pytz.UTC).timestamp()).
        add_elements(
            elements=elem,
            event_timestamp=datetime(
                2021, 3, 1, 0, 0, 17, 0,
                tzinfo=pytz.UTC).timestamp()).add_elements(
                    elements=elem,
                    event_timestamp=datetime(
                        2021, 3, 1, 0, 0, 18, 0,
                        tzinfo=pytz.UTC).timestamp()).add_elements(
                            elements=elem,
                            event_timestamp=datetime(
                                2021, 3, 1, 0, 0, 19, 0,
                                tzinfo=pytz.UTC).timestamp()).
        advance_watermark_to(
            datetime(2021, 3, 1, 0, 0, 20, 0,
                     tzinfo=pytz.UTC).timestamp()).add_elements(
                         elements=elem,
                         event_timestamp=datetime(
                             2021, 3, 1, 0, 0, 20, 0,
                             tzinfo=pytz.UTC).timestamp()).advance_watermark_to(
                                 datetime(
                                     2021, 3, 1, 0, 0, 25, 0, tzinfo=pytz.UTC).
                                 timestamp()).advance_watermark_to_infinity())
