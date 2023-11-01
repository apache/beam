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

"""This module defines the basic MapToFields operation."""
import itertools
from collections import abc
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import Mapping
from typing import Optional
from typing import Union

import js2py
from js2py import base
from js2py.constructors import jsdate
from js2py.internals import simplex

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import row_type
from apache_beam.typehints import schemas
from apache_beam.typehints import trivial_inference
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.utils import python_callable
from apache_beam.yaml import json_utils
from apache_beam.yaml import options
from apache_beam.yaml import yaml_provider
from apache_beam.yaml.yaml_provider import dicts_to_rows


def _check_mapping_arguments(
    transform_name, expression=None, callable=None, name=None, path=None):
  # Argument checking
  if not expression and not callable and not path and not name:
    raise ValueError(
        f'{transform_name} must specify either "expression", "callable", '
        f'or both "path" and "name"')
  if expression and callable:
    raise ValueError(
        f'{transform_name} cannot specify both "expression" and "callable"')
  if (expression or callable) and (path or name):
    raise ValueError(
        f'{transform_name} cannot specify "expression" or "callable" with '
        f'"path" or "name"')
  if path and not name:
    raise ValueError(f'{transform_name} cannot specify "path" without "name"')
  if name and not path:
    raise ValueError(f'{transform_name} cannot specify "name" without "path"')


# js2py's JsObjectWrapper object has a self-referencing __dict__ property
# that cannot be pickled without implementing the __getstate__ and
# __setstate__ methods.
class _CustomJsObjectWrapper(js2py.base.JsObjectWrapper):
  def __init__(self, js_obj):
    super().__init__(js_obj.__dict__['_obj'])

  def __getstate__(self):
    return self.__dict__.copy()

  def __setstate__(self, state):
    self.__dict__.update(state)


# TODO(yaml) Improve type inferencing for JS UDF's
def py_value_to_js_dict(py_value):
  if ((isinstance(py_value, tuple) and hasattr(py_value, '_asdict')) or
      isinstance(py_value, beam.Row)):
    py_value = py_value._asdict()
  if isinstance(py_value, dict):
    return {key: py_value_to_js_dict(value) for key, value in py_value.items()}
  elif not isinstance(py_value, str) and isinstance(py_value, abc.Iterable):
    return [py_value_to_js_dict(value) for value in list(py_value)]
  else:
    return py_value


# TODO(yaml) Consider adding optional language version parameter to support
#  ECMAScript 5 and 6
def _expand_javascript_mapping_func(
    original_fields, expression=None, callable=None, path=None, name=None):

  js_array_type = (
      base.PyJsArray,
      base.PyJsArrayBuffer,
      base.PyJsInt8Array,
      base.PyJsUint8Array,
      base.PyJsUint8ClampedArray,
      base.PyJsInt16Array,
      base.PyJsUint16Array,
      base.PyJsInt32Array,
      base.PyJsUint32Array,
      base.PyJsFloat32Array,
      base.PyJsFloat64Array)

  def _js_object_to_py_object(obj):
    if isinstance(obj, (base.PyJsNumber, base.PyJsString, base.PyJsBoolean)):
      return base.to_python(obj)
    elif isinstance(obj, js_array_type):
      return [_js_object_to_py_object(value) for value in obj.to_list()]
    elif isinstance(obj, jsdate.PyJsDate):
      return obj.to_utc_dt()
    elif isinstance(obj, (base.PyJsNull, base.PyJsUndefined)):
      return None
    elif isinstance(obj, base.PyJsError):
      raise RuntimeError(obj['message'])
    elif isinstance(obj, base.PyJsObject):
      return {
          key: _js_object_to_py_object(value['value'])
          for (key, value) in obj.own.items()
      }
    elif isinstance(obj, base.JsObjectWrapper):
      return _js_object_to_py_object(obj._obj)

    return obj

  if expression:
    source = '\n'.join(['function(__row__) {'] + [
        f'  {name} = __row__.{name}'
        for name in original_fields if name in expression
    ] + ['  return (' + expression + ')'] + ['}'])
    js_func = _CustomJsObjectWrapper(js2py.eval_js(source))

  elif callable:
    js_func = _CustomJsObjectWrapper(js2py.eval_js(callable))

  else:
    if not path.endswith('.js'):
      raise ValueError(f'File "{path}" is not a valid .js file.')
    udf_code = FileSystems.open(path).read().decode()
    js = js2py.EvalJs()
    js.eval(udf_code)
    js_func = _CustomJsObjectWrapper(getattr(js, name))

  def js_wrapper(row):
    row_as_dict = py_value_to_js_dict(row)
    try:
      js_result = js_func(row_as_dict)
    except simplex.JsException as exn:
      raise RuntimeError(
          f"Error evaluating javascript expression: "
          f"{exn.mes['message']}") from exn
    return dicts_to_rows(_js_object_to_py_object(js_result))

  return js_wrapper


def _expand_python_mapping_func(
    original_fields, expression=None, callable=None, path=None, name=None):
  if path and name:
    if not path.endswith('.py'):
      raise ValueError(f'File "{path}" is not a valid .py file.')
    py_file = FileSystems.open(path).read().decode()

    return python_callable.PythonCallableWithSource.load_from_script(
        py_file, name)

  elif expression:
    # TODO(robertwb): Consider constructing a single callable that takes
    # the row and returns the new row, rather than invoking (and unpacking)
    # for each field individually.
    source = '\n'.join(['def fn(__row__):'] + [
        f'  {name} = __row__.{name}'
        for name in original_fields if name in expression
    ] + ['  return (' + expression + ')'])

  else:
    source = callable

  return python_callable.PythonCallableWithSource(source)


def _validator(beam_type: schema_pb2.FieldType) -> Callable[[Any], bool]:
  """Returns a callable converting rows of the given type to Json objects."""
  type_info = beam_type.WhichOneof("type_info")
  if type_info == "atomic_type":
    if beam_type.atomic_type == schema_pb2.BOOLEAN:
      return lambda x: isinstance(x, bool)
    elif beam_type.atomic_type == schema_pb2.INT64:
      return lambda x: isinstance(x, int)
    elif beam_type.atomic_type == schema_pb2.DOUBLE:
      return lambda x: isinstance(x, (int, float))
    elif beam_type.atomic_type == schema_pb2.STRING:
      return lambda x: isinstance(x, str)
    else:
      raise ValueError(
          f'Unknown or unsupported atomic type: {beam_type.atomic_type}')
  elif type_info == "array_type":
    element_validator = _validator(beam_type.array_type.element_type)
    return lambda value: all(element_validator(e) for e in value)
  elif type_info == "iterable_type":
    element_validator = _validator(beam_type.iterable_type.element_type)
    return lambda value: all(element_validator(e) for e in value)
  elif type_info == "map_type":
    key_validator = _validator(beam_type.map_type.key_type)
    value_validator = _validator(beam_type.map_type.value_type)
    return lambda value: all(
        key_validator(k) and value_validator(v) for (k, v) in value.items())
  elif type_info == "row_type":
    validators = {
        field.name: _validator(field.type)
        for field in beam_type.row_type.schema.fields
    }
    return lambda row: all(
        validator(getattr(row, name))
        for (name, validator) in validators.items())
  else:
    raise ValueError(f"Unrecognized type_info: {type_info!r}")


def _as_callable(original_fields, expr, transform_name, language):
  if expr in original_fields:
    return expr

  # TODO(yaml): support an imports parameter
  # TODO(yaml): support a requirements parameter (possibly at a higher level)
  if isinstance(expr, str):
    expr = {'expression': expr}
  if not isinstance(expr, dict):
    raise ValueError(
        f"Ambiguous expression type (perhaps missing quoting?): {expr}")
  explicit_type = expr.pop('output_type', None)
  _check_mapping_arguments(transform_name, **expr)

  if language == "javascript":
    func = _expand_javascript_mapping_func(original_fields, **expr)
  elif language == "python":
    func = _expand_python_mapping_func(original_fields, **expr)
  else:
    raise ValueError(
        f'Unknown language for mapping transform: {language}. '
        'Supported languages are "javascript" and "python."')

  if explicit_type:
    if isinstance(explicit_type, str):
      explicit_type = {'type': explicit_type}
    beam_type = json_utils.json_type_to_beam_type(explicit_type)
    validator = _validator(beam_type)

    @beam.typehints.with_output_types(schemas.typing_from_runner_api(beam_type))
    def checking_func(row):
      result = func(row)
      if not validator(result):
        raise TypeError(f'{result} violates schema {explicit_type}')
      return result

    return checking_func

  else:
    return func


def exception_handling_args(error_handling_spec):
  if error_handling_spec:
    return {
        'dead_letter_tag' if k == 'output' else k: v
        for (k, v) in error_handling_spec.items()
    }
  else:
    return None


def _map_errors_to_standard_format():
  # TODO(https://github.com/apache/beam/issues/24755): Switch to MapTuple.
  return beam.Map(
      lambda x: beam.Row(element=x[0], msg=str(x[1][1]), stack=str(x[1][2])))


def maybe_with_exception_handling(inner_expand):
  def expand(self, pcoll):
    wrapped_pcoll = beam.core._MaybePValueWithErrors(
        pcoll, self._exception_handling_args)
    return inner_expand(self, wrapped_pcoll).as_result(
        _map_errors_to_standard_format())

  return expand


def maybe_with_exception_handling_transform_fn(transform_fn):
  def expand(pcoll, error_handling=None, **kwargs):
    wrapped_pcoll = beam.core._MaybePValueWithErrors(
        pcoll, exception_handling_args(error_handling))
    return transform_fn(wrapped_pcoll,
                        **kwargs).as_result(_map_errors_to_standard_format())

  return expand


class _Explode(beam.PTransform):
  def __init__(
      self,
      fields: Union[str, Collection[str]],
      cross_product: Optional[bool] = None,
      error_handling: Optional[Mapping[str, Any]] = None):
    if isinstance(fields, str):
      fields = [fields]
    if cross_product is None:
      if len(fields) > 1:
        raise ValueError(
            'cross_product must be specified true or false '
            'when exploding multiple fields')
      else:
        # Doesn't matter.
        cross_product = True
    self._fields = fields
    self._cross_product = cross_product
    # TODO(yaml): Support standard error handling argument.
    self._exception_handling_args = exception_handling_args(error_handling)

  @maybe_with_exception_handling
  def expand(self, pcoll):
    all_fields = [
        x for x, _ in named_fields_from_element_type(pcoll.element_type)
    ]
    for field in self._fields:
      if field not in all_fields:
        raise ValueError(f'Exploding unknown field "{field}"')
    to_explode = self._fields

    def explode_cross_product(base, fields):
      if fields:
        copy = dict(base)
        for value in base[fields[0]]:
          copy[fields[0]] = value
          yield from explode_cross_product(copy, fields[1:])
      else:
        yield beam.Row(**base)

    def explode_zip(base, fields):
      to_zip = [base[field] for field in fields]
      copy = dict(base)
      for values in itertools.zip_longest(*to_zip, fillvalue=None):
        for ix, field in enumerate(fields):
          copy[field] = values[ix]
        yield beam.Row(**copy)

    cross_product = self._cross_product
    return (
        pcoll
        | beam.FlatMap(
            lambda row:
            (explode_cross_product if cross_product else explode_zip)
            ({name: getattr(row, name)
              for name in all_fields}, to_explode)))

  def infer_output_type(self, input_type):
    return row_type.RowTypeConstraint.from_fields([(
        name,
        trivial_inference.element_type(typ) if name in self._fields else
        typ) for (name, typ) in named_fields_from_element_type(input_type)])

  def with_exception_handling(self, **kwargs):
    # It's possible there's an error in iteration...
    self._exception_handling_args = kwargs
    return self


@beam.ptransform.ptransform_fn
@maybe_with_exception_handling_transform_fn
def _PyJsFilter(
    pcoll, keep: Union[str, Dict[str, str]], language: Optional[str] = None):
  if language == 'javascript':
    options.YamlOptions.check_enabled(pcoll.pipeline, 'javascript')

  try:
    input_schema = dict(named_fields_from_element_type(pcoll.element_type))
  except (TypeError, ValueError) as exn:
    if is_expr(keep):
      raise ValueError("Can only use expressions on a schema'd input.") from exn
    input_schema = {}  # unused

  if isinstance(keep, str) and keep in input_schema:
    keep_fn = lambda row: getattr(row, keep)
  else:
    keep_fn = _as_callable(list(input_schema.keys()), keep, "keep", language)
  return pcoll | beam.Filter(keep_fn)


def is_expr(v):
  return isinstance(v, str) or (isinstance(v, dict) and 'expression' in v)


def normalize_fields(pcoll, fields, drop=(), append=False, language='generic'):
  try:
    input_schema = dict(named_fields_from_element_type(pcoll.element_type))
  except (TypeError, ValueError) as exn:
    if drop:
      raise ValueError("Can only drop fields on a schema'd input.") from exn
    if append:
      raise ValueError("Can only append fields on a schema'd input.") from exn
    elif any(is_expr(x) for x in fields.values()):
      raise ValueError("Can only use expressions on a schema'd input.") from exn
    input_schema = {}

  if isinstance(drop, str):
    drop = [drop]
  if drop and not append:
    raise ValueError("Can only drop fields if append is true.")
  for name in drop:
    if name not in input_schema:
      raise ValueError(f'Dropping unknown field "{name}"')
  if append:
    for name in fields:
      if name in input_schema and name not in drop:
        raise ValueError(
            f'Redefinition of field "{name}". '
            'Cannot append a field that already exists in original input.')

  if language == 'generic':
    for expr in fields.values():
      if not isinstance(expr, str):
        raise ValueError(
            "Missing language specification. "
            "Must specify a language when using a map with custom logic.")
    missing = set(fields.values()) - set(input_schema.keys())
    if missing:
      raise ValueError(
          f"Missing language specification or unknown input fields: {missing}")

  if append:
    return input_schema, {
        **{name: name
           for name in input_schema.keys() if name not in drop},
        **fields
    }
  else:
    return input_schema, fields


@beam.ptransform.ptransform_fn
@maybe_with_exception_handling_transform_fn
def _PyJsMapToFields(pcoll, language='generic', **mapping_args):
  input_schema, fields = normalize_fields(
      pcoll, language=language, **mapping_args)
  if language == 'javascript':
    options.YamlOptions.check_enabled(pcoll.pipeline, 'javascript')

  original_fields = list(input_schema.keys())

  return pcoll | beam.Select(
      **{
          name: _as_callable(original_fields, expr, name, language)
          for (name, expr) in fields.items()
      })


@beam.ptransform.ptransform_fn
def _SqlFilterTransform(pcoll, sql_transform_constructor, keep, language):
  return pcoll | sql_transform_constructor(
      f'SELECT * FROM PCOLLECTION WHERE {keep}')


@beam.ptransform.ptransform_fn
def _SqlMapToFieldsTransform(pcoll, sql_transform_constructor, **mapping_args):
  _, fields = normalize_fields(pcoll, **mapping_args)

  def extract_expr(name, v):
    if isinstance(v, str):
      return v
    elif 'expression' in v:
      return v['expression']
    else:
      raise ValueError("Only expressions allowed in SQL at {name}.")

  selects = [
      f'({extract_expr(name, expr)}) AS {name}'
      for (name, expr) in fields.items()
  ]
  query = "SELECT " + ", ".join(selects) + " FROM PCOLLECTION"
  return pcoll | sql_transform_constructor(query)


def create_mapping_providers():
  # These are MetaInlineProviders because their expansion is in terms of other
  # YamlTransforms, but in a way that needs to be deferred until the input
  # schema is known.
  return [
      yaml_provider.InlineProvider({
          'Explode': _Explode,
          'Filter-python': _PyJsFilter,
          'Filter-javascript': _PyJsFilter,
          'MapToFields-python': _PyJsMapToFields,
          'MapToFields-javascript': _PyJsMapToFields,
          'MapToFields-generic': _PyJsMapToFields,
      }),
      yaml_provider.SqlBackedProvider({
          'Filter-sql': _SqlFilterTransform,
          'Filter-calcite': _SqlFilterTransform,
          'MapToFields-sql': _SqlMapToFieldsTransform,
          'MapToFields-calcite': _SqlMapToFieldsTransform,
      }),
  ]
