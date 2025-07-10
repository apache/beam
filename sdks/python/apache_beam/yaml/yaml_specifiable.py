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

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.transforms import AnomalyDetection
from apache_beam.ml.anomaly.transforms import Specifiable
from apache_beam.utils import python_callable
from apache_beam.yaml.yaml_provider import InlineProvider


def maybe_make_specifiable(v):
  if isinstance(v, dict):
    if "type" in v and "config" in v:
      return Specifiable.from_spec(
          Spec(type=v["type"], config=maybe_make_specifiable(v["config"])))

    if "callable" in v:
      if "path" in v or "name" in v:
        raise ValueError(
            "Cannot specify 'callable' with 'path' and 'name' for function.")
      else:
        return python_callable.PythonCallableWithSource(v["callable"])

    if "path" in v and "name" in v:
      return python_callable.PythonCallableWithSource.load_from_script(
          FileSystems.open(v["path"]).read().decode(), v["name"])

    ret = {k: maybe_make_specifiable(v[k]) for k in v}
    return ret
  else:
    return v


class SpecProvider(InlineProvider):
  def create_transform(self, type, args, yaml_create_transform):
    return self._transform_factories[type](
        **{
            k: maybe_make_specifiable(v)
            for k, v in args.items()
        })


def create_spec_providers():
  return SpecProvider({"AnomalyDetection": AnomalyDetection})
