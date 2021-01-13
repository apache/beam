#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import apache_beam as beam
from apache_beam import pvalue

from log_elements import LogElements

num_below_100_tag = 'num_below_100'
num_above_100_tag = 'num_above_100'


class ProcessNumbersDoFn(beam.DoFn):

    def process(self, element):
        if element <= 100:
            yield element
        else:
            yield pvalue.TaggedOutput(num_above_100_tag, element)


with beam.Pipeline() as p:

  results = \
      (p | beam.Create([10, 50, 120, 20, 200, 0])
         | beam.ParDo(ProcessNumbersDoFn())
          .with_outputs(num_above_100_tag, main=num_below_100_tag))

  results[num_below_100_tag] | 'Log numbers <= 100' >> LogElements(prefix='Number <= 100: ')
  results[num_above_100_tag] | 'Log numbers > 100' >> LogElements(prefix='Number > 100: ')

