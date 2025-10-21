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

"""JSON utilities for the Nexmark suite."""

import json

from apache_beam.utils.timestamp import Timestamp


def model_to_json(model, nexmark_types):
  return json.dumps(
      construct_json_dict(model, nexmark_types), separators=(',', ':'))


def construct_json_dict(model, nexmark_types):
  return {
      k: unnest_to_json(v, nexmark_types)
      for k, v in model.__dict__.items()
  }


def unnest_to_json(cand, nexmark_types):
  if isinstance(cand, Timestamp):
    return cand.micros // 1000
  elif isinstance(cand, nexmark_types):
    return construct_json_dict(cand, nexmark_types)
  else:
    return cand


def millis_to_timestamp(millis: int) -> Timestamp:
  micro_second = millis * 1000
  return Timestamp(micros=micro_second)
