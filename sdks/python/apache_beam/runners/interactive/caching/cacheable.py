#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Module for dataclasses to hold metadata of cacheable PCollections in the user
code scope.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

from dataclasses import dataclass

import apache_beam as beam


@dataclass
class Cacheable:
  var: str
  version: str
  producer_version: str
  pcoll: beam.pvalue.PCollection

  def __hash__(self):
    return hash((self.var, self.version, self.producer_version, self.pcoll))

  @staticmethod
  def from_pcoll(
      pcoll_name: str, pcoll: beam.pvalue.PCollection) -> 'Cacheable':
    return Cacheable(pcoll_name, str(id(pcoll)), str(id(pcoll.producer)), pcoll)

  def to_key(self):
    return CacheKey(
        self.var,
        self.version,
        self.producer_version,
        str(id(self.pcoll.pipeline)))


@dataclass
class CacheKey:
  """The identifier of a cacheable PCollection in cache.

  It contains 4 stringified components:
  var: The obfuscated variable name of the PCollection.
  version: The id() of the PCollection.
  producer_version: The id() of the producer of the PCollection.
  pipeline_id: The id() of the pipeline the PCollection belongs to.
  """
  var: str
  version: str
  producer_version: str
  pipeline_id: str

  def __post_init__(self):
    from apache_beam.runners.interactive.utils import obfuscate
    # Normalize arbitrary variable name to a fixed length hex str.
    self.var = obfuscate(self.var)[:10]

  def __hash__(self):
    return hash(
        (self.var, self.version, self.producer_version, self.pipeline_id))

  @staticmethod
  def from_str(r: str) -> 'CacheKey':
    r_split = r.split('-')
    ck = CacheKey(*r_split)
    # Avoid double obfuscation.
    ck.var = r_split[0]
    return ck

  @staticmethod
  def from_pcoll(pcoll_name: str, pcoll: beam.pvalue.PCollection) -> 'CacheKey':
    return CacheKey(
        pcoll_name,
        str(id(pcoll)),
        str(id(pcoll.producer)),
        str(id(pcoll.pipeline)))

  def to_str(self):
    return '-'.join(
        [self.var, self.version, self.producer_version, self.pipeline_id])

  def __repr__(self):
    return self.to_str()

  def __str__(self):
    return self.to_str()
