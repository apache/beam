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


class Person:
    def __init__(self, name, city, country=''):
        self.name = name
        self.city = city
        self.country = country

    def __str__(self):
        return 'Person[' + self.name + ',' + self.city + ',' + self.country + ']'


class EnrichCountryDoFn(beam.DoFn):

    def process(self, element, cities_to_countries):
        yield Person(element.name, element.city,
                     cities_to_countries[element.city])


with beam.Pipeline() as p:

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
     | LogElements())

