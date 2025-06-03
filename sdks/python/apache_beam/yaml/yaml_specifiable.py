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

import inspect

import apache_beam as beam
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.transforms import AnomalyDetection
from apache_beam.ml.anomaly.transforms import Specifiable
from apache_beam.yaml.yaml_provider import InlineProvider


def dict_to_maybe_specifiable(d: dict):
  def specify_helper(d: dict):
    ret = {}
    for k, v in d.items():
      if isinstance(v, dict):
        ret[k] = dict_to_maybe_specifiable(v)
      else:
        ret[k] = v
    return ret

  if "type" in d and "config" in d and isinstance(d["config"], dict):
    return Specifiable.from_spec(
        Spec(type=d["type"], config=specify_helper(d["config"])))
  else:
    return specify_helper(d)


class SpecifiableTransform(beam.PTransform):
  AVAILABlE_TRANSFORMS = {}

  @staticmethod
  def register(typ, cls):
    SpecifiableTransform.AVAILABlE_TRANSFORMS[typ] = cls

  def __init__(self, **kwargs):
    self._typ = inspect.currentframe().f_back.f_locals.get("type", None)
    assert self._typ is not None

    self._kwargs = dict_to_maybe_specifiable(kwargs)

  def expand(self, pcoll):
    if self._typ in SpecifiableTransform.AVAILABlE_TRANSFORMS:
      return pcoll | SpecifiableTransform.AVAILABlE_TRANSFORMS[self._typ](
          **self._kwargs)


SpecifiableTransform.register("AnomalyDetection", AnomalyDetection)


def create_specifiable_providers():
  return InlineProvider({
      k: SpecifiableTransform
      for k in SpecifiableTransform.AVAILABlE_TRANSFORMS
  })
