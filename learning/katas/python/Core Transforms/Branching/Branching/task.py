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

from log_elements import LogElements

with beam.Pipeline() as p:

  numbers = p | beam.Create([1, 2, 3, 4, 5])

  mult5_results = numbers | beam.Map(lambda num: num * 5)
  mult10_results = numbers | beam.Map(lambda num: num * 10)

  mult5_results | 'Log multiply 5' >> LogElements(prefix='Multiplied by 5: ')
  mult10_results | 'Log multiply 10' >> LogElements(prefix='Multiplied by 10: ')

