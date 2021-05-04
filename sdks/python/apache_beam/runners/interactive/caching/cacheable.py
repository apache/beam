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
from apache_beam.runners.interactive.utils import obfuscate


@dataclass
class Cacheable:
  pcoll_id: str
  var: str
  version: str
  pcoll: beam.pvalue.PCollection
  producer_version: str

  def __hash__(self):
    return hash((
        self.pcoll_id,
        self.var,
        self.version,
        self.pcoll,
        self.producer_version))

  def to_key(self):
    return CacheKey(
        self.var,
        self.version,
        self.producer_version,
        str(id(self.pcoll.pipeline)))


@dataclass
class CacheKey:
  var: str
  version: str
  producer_version: str
  pipeline_id: str

  def __post_init__(self):
    # Normalize arbitrary variable name to a fixed length hex str.
    self.var = obfuscate(self.var)[:10]

  @staticmethod
  def from_str(r):
    r_split = r.split('-')
    ck = CacheKey(*r_split)
    ck.var = r_split[0]
    return ck

  def __repr__(self):
    return '-'.join(
        [self.var, self.version, self.producer_version, self.pipeline_id])
