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
#   name: side-inputs
#   description: Side-inputs example.
#   multifile: false
#   context_line: 64
#   categories:
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - hellobeam

import apache_beam as beam

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

class Person:
    def __init__(self, name, city, country=''):
        self.name = name
        self.city = city
        self.country = country

    def __str__(self):
        return 'Person[' + self.name + ',' + self.city + ',' + self.country + ']'


class EnrichCountryDoFn(beam.DoFn):
    # Get city from cities_to_countries and set person
    def process(self, element, cities_to_countries):
        yield Person(element.name, element.city,
                     cities_to_countries[element.city])


with beam.Pipeline() as p:
  # List of elements
  cities_to_countries = {
        'Beijing': 'China',
        'London': 'United Kingdom',
        'San Francisco': 'United States',
        'Singapore': 'Singapore',
        'Sydney': 'Australia'
    }

  persons = [
        Person('Henry', 'Singapore'),
        Person('Jane', 'San Francisco'),
        Person('Lee', 'Beijing'),
        Person('John', 'Sydney'),
        Person('Alfred', 'London')
    ]

  (p | beam.Create(persons)
     | beam.ParDo(EnrichCountryDoFn(), cities_to_countries)
     | Output())