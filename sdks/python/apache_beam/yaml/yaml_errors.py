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

import functools
import inspect
from typing import NamedTuple

import apache_beam as beam
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.yaml.yaml_utils import SafeLineLoader


class ErrorHandlingConfig(NamedTuple):
  """This option specifies whether and where to output error rows.

      Args:
        output (str): Name to use for the output error collection
      """
  output: str
  # TODO: Other parameters are valid here too, but not common to Java.


def exception_handling_args(error_handling_spec):
  if error_handling_spec:
    # error_handling_spec may have come from a yaml file and have metadata.
    clean_spec = SafeLineLoader.strip_metadata(error_handling_spec)
    return {
        'dead_letter_tag' if k == 'output' else k: v
        for (k, v) in clean_spec.items()
    }
  else:
    return None


def map_errors_to_standard_format(input_type):
  # TODO(https://github.com/apache/beam/issues/24755): Switch to MapTuple.

  return beam.Map(
      lambda x: beam.Row(
          element=x[0], msg=str(x[1][1]), stack=''.join(x[1][2]))
  ).with_output_types(
      RowTypeConstraint.from_fields([("element", input_type), ("msg", str),
                                     ("stack", str)]))


def maybe_with_exception_handling(inner_expand):
  def expand(self, pcoll):
    wrapped_pcoll = beam.core._MaybePValueWithErrors(
        pcoll, self._exception_handling_args)
    return inner_expand(self, wrapped_pcoll).as_result(
        map_errors_to_standard_format(pcoll.element_type))

  return expand


def maybe_with_exception_handling_transform_fn(transform_fn):
  @functools.wraps(transform_fn)
  def expand(pcoll, error_handling=None, **kwargs):
    wrapped_pcoll = beam.core._MaybePValueWithErrors(
        pcoll, exception_handling_args(error_handling))
    return transform_fn(wrapped_pcoll, **kwargs).as_result(
        map_errors_to_standard_format(pcoll.element_type))

  original_signature = inspect.signature(transform_fn)
  new_parameters = list(original_signature.parameters.values())
  error_handling_param = inspect.Parameter(
      'error_handling',
      inspect.Parameter.KEYWORD_ONLY,
      default=None,
      annotation=ErrorHandlingConfig)
  if new_parameters[-1].kind == inspect.Parameter.VAR_KEYWORD:
    new_parameters.insert(-1, error_handling_param)
  else:
    new_parameters.append(error_handling_param)
  expand.__signature__ = original_signature.replace(parameters=new_parameters)

  return expand
