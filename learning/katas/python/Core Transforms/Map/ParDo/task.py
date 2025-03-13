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
#   name: MapPardo
#   description: Task from katas is simple ParDo that maps the input element by multiplying it by 10.
#   multifile: false
#   context_line: 38
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - numbers

import apache_beam as beam


class MultiplyByTenDoFn(beam.DoFn):

    def process(self, element):
        yield element * 10


with beam.Pipeline() as p:

  (p | beam.Create([1, 2, 3, 4, 5])
     | beam.ParDo(MultiplyByTenDoFn())
     | beam.LogElements())

