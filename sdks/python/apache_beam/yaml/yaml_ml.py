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

"""This module defines yaml wrappings for some ML transforms."""

from typing import Any
from typing import List
from typing import Optional

import apache_beam as beam
from apache_beam.yaml import options

try:
  from apache_beam.ml.transforms import tft
  from apache_beam.ml.transforms.base import MLTransform
  # TODO(robertwb): Is this all of them?
  _transform_constructors = tft.__dict__
except ImportError:
  tft = None  # type: ignore


def _config_to_obj(spec):
  if 'type' not in spec:
    raise ValueError(r"Missing type in ML transform spec {spec}")
  if 'config' not in spec:
    raise ValueError(r"Missing config in ML transform spec {spec}")
  constructor = _transform_constructors.get(spec['type'])
  if constructor is None:
    raise ValueError("Unknown ML transform type: %r" % spec['type'])
  return constructor(**spec['config'])


@beam.ptransform.ptransform_fn
def ml_transform(
    pcoll,
    write_artifact_location: Optional[str] = None,
    read_artifact_location: Optional[str] = None,
    transforms: Optional[List[Any]] = None):
  if tft is None:
    raise ValueError(
        'tensorflow-transform must be installed to use this MLTransform')
  options.YamlOptions.check_enabled(pcoll.pipeline, 'ML')
  # TODO(robertwb): Perhaps _config_to_obj could be pushed into MLTransform
  # itself for better cross-language support?
  return pcoll | MLTransform(
      write_artifact_location=write_artifact_location,
      read_artifact_location=read_artifact_location,
      transforms=[_config_to_obj(t) for t in transforms] if transforms else [])


if tft is not None:
  ml_transform.__doc__ = MLTransform.__doc__
