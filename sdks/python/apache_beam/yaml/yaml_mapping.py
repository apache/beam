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
import re
from collections import abc
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import TypeVar
from typing import Union

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.portability.api import schema_pb2
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints import row_type
from apache_beam.typehints import schemas
from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.typehints.schemas import schema_from_element_type
from apache_beam.typehints.schemas import typing_from_runner_api
from apache_beam.utils import python_callable
from apache_beam.yaml import json_utils
from apache_beam.yaml import options
from apache_beam.yaml import yaml_provider
from apache_beam.yaml.yaml_errors import exception_handling_args
from apache_beam.yaml.yaml_errors import map_errors_to_standard_format
from apache_beam.yaml.yaml_errors import maybe_with_exception_handling
from apache_beam.yaml.yaml_errors import maybe_with_exception_handling_transform_fn
from apache_beam.yaml.yaml_provider import dicts_to_rows

# Import js2py package if it exists
try:
  import js2py
  from js2py.base import JsObjectWrapper
except ImportError:
  js2py = None
  JsObjectWrapper = object

_str_expression_fields = {
    'AssignTimestamps': 'timestamp',
    'Filter': 'keep',
    'Partition': 'by',
}


def normalize_mapping(spec):
  """
  Normalizes various fields for mapping transforms.
  """
  if spec['type'] == 'MapToFields':
    config = spec.get('config')
    if isinstance(config.get('drop'), str):
      config['drop'] = [config['drop']]
    for field, value in list(config.get('fields', {}).items()):
      if isinstance(value, (str, int, float)):
        config['fields'][field] = {'expression': str(value)}

  elif spec['type'] in _str_expression_fields:
    param = _str_expression_fields[spec['type']]
    config = spec.get('config', {})
    if isinstance(config.get(param), (str, int, float)):
      config[param] = {'expression': str(config.get(param))}

  return spec


def is_literal(expr: str) -> bool:
  # Some languages have limited integer literal ranges.
  if re.fullmatch(r'-?\d+?', expr) and -1 << 31 < int(expr) < 1 << 31:
    return True
  elif re.fullmatch(r'-?\d+\.\d*', expr):
    return True
  elif re.fullmatch(r'"[^\\"]*"', expr):
    return True
  else:
    return False


def validate_generic_expression(
    expr_dict: dict,
    input_fields: Collection[str],
    allow_cmp: bool,
    error_field: str) -> None:
  if not isinstance(expr_dict, dict):
    raise ValueError(
        f"Ambiguous expression type (perhaps missing quoting?): {expr_dict}")
  if len(expr_dict) != 1 or 'expression' not in expr_dict:
    raise ValueError(
        "Missing language specification. "
        "Must specify a language when using a map with custom logic for %s" %
        error_field)
  expr = str(expr_dict['expression'])

  def is_atomic(expr: str):
    return is_literal(expr) or expr in input_fields

  if is_atomic(expr):
    return

  if allow_cmp:
    maybe_cmp = re.fullmatch('(.*)([<>=!]+)(.*)', expr)
    if maybe_cmp:
      left, cmp, right = maybe_cmp.groups()
      if (is_atomic(left.strip()) and is_atomic(right.strip()) and
          cmp in {'==', '<=', '>=', '<', '>', '!='}):
        return

  raise ValueError(
      "Missing language specification, unknown input fields, "
      f"or invalid generic expression: {expr}. "
      "See https://beam.apache.org/documentation/sdks/yaml-udf/#generic")


def validate_generic_expressions(base_type, config, input_pcolls) -> None:
  if not input_pcolls:
    return
  try:
    input_fields = [
        name for (name, _) in named_fields_from_element_type(
            next(iter(input_pcolls)).element_type)
    ]
  except (TypeError, ValueError):
    input_fields = []

  if base_type == 'MapToFields':
    for field, value in list(config.get('fields', {}).items()):
      validate_generic_expression(value, input_fields, True, field)

  elif base_type in _str_expression_fields:
    param = _str_expression_fields[base_type]
    validate_generic_expression(
        config.get(param), input_fields, base_type == 'Filter', param)


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
class _CustomJsObjectWrapper(JsObjectWrapper):
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

  # Check for installed js2py package
  if js2py is None:
    raise ValueError(
        "Javascript mapping functions are not supported on"
        " Python 3.12 or later.")

  # import remaining js2py objects
  from js2py import base
  from js2py.constructors import jsdate
  from js2py.internals import simplex

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
    source = '\n'.join(['def fn(__row__):'] + ['  try:'] + [
        f'    {name} = __row__.{name}'
        for name in original_fields if name in expression
    ] + [f'    return ({expression})'] + ['  except NameError as e:'] + [
        f'    raise ValueError(f"{{e}}. Valid values include '
        f'{original_fields}")'
    ])

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


def _as_callable_for_pcoll(
    pcoll,
    fn_spec: Union[str, Dict[str, str]],
    msg: str,
    language: Optional[str]):
  if language == 'javascript':
    options.YamlOptions.check_enabled(pcoll.pipeline, 'javascript')

  try:
    input_schema = dict(named_fields_from_element_type(pcoll.element_type))
  except (TypeError, ValueError) as exn:
    if is_expr(fn_spec):
      raise ValueError("Can only use expressions on a schema'd input.") from exn
    input_schema = {}  # unused

  if isinstance(fn_spec, str) and fn_spec in input_schema:
    return lambda row: getattr(row, fn_spec)
  else:
    return _as_callable(
        list(input_schema.keys()), fn_spec, msg, language, input_schema)


def _as_callable(original_fields, expr, transform_name, language, input_schema):
  if isinstance(expr, str):
    expr = {'expression': expr}

  # Extract original type from upstream pcoll when doing simple mappings
  original_type = input_schema.get(expr.get('expression'), None)
  if expr in original_fields:
    language = "python"

  # TODO(yaml): support an imports parameter
  # TODO(yaml): support a requirements parameter (possibly at a higher level)
  if not isinstance(expr, dict):
    raise ValueError(
        f"Ambiguous expression type (perhaps missing quoting?): {expr}")
  explicit_type = expr.pop('output_type', None)
  _check_mapping_arguments(transform_name, **expr)

  if language == "javascript":
    func = _expand_javascript_mapping_func(original_fields, **expr)
  elif language in ("python", "generic", None):
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

  elif original_type:
    return beam.typehints.with_output_types(
        convert_to_beam_type(original_type))(
            func)

  else:
    return func


class _StripErrorMetadata(beam.PTransform):
  """Strips error metadata from outputs returned via error handling.

  Generally the error outputs for transformations return information about
  the error encountered (e.g. error messages and tracebacks) in addition to the
  failing element itself.  This transformation attempts to remove that metadata
  and returns the bad element alone which can be useful for re-processing.

  For example, in the following pipeline snippet::

      - name: MyMappingTransform
        type: MapToFields
        input: SomeInput
        config:
          language: python
          fields:
            ...
          error_handling:
            output: errors

      - name: RecoverOriginalElements
        type: StripErrorMetadata
        input: MyMappingTransform.errors

  the output of `RecoverOriginalElements` will contain exactly those elements
  from SomeInput that failed to processes (whereas `MyMappingTransform.errors`
  would contain those elements paired with error information).

  Note that this relies on the preceding transform actually returning the
  failing input in a schema'd way.  Most built-in transformation follow the
  correct conventions.
  """

  _ERROR_FIELD_NAMES = ('failed_row', 'element', 'record')

  def __init__(self):
    super().__init__(label=None)

  def expand(self, pcoll):
    try:
      existing_fields = {
          fld.name: fld.type
          for fld in schema_from_element_type(pcoll.element_type).fields
      }
    except TypeError:
      fld = None
    else:
      for fld in self._ERROR_FIELD_NAMES:
        if fld in existing_fields:
          break
      else:
        raise ValueError(
            'The input to this transform does not appear to be an error ' +
            "output.  Expected a schema'd input with a field named " +
            ' or '.join(repr(fld) for fld in self._ERROR_FIELD_NAMES))

    if fld is None:
      # This handles with_exception_handling() that returns bare tuples.
      return pcoll | beam.Map(lambda x: x[0])
    else:
      return pcoll | beam.Map(lambda x: getattr(x, fld)).with_output_types(
          typing_from_runner_api(existing_fields[fld]))


class _Validate(beam.PTransform):
  """Validates each element of a PCollection against a json schema.

  Args:
      schema: A json schema against which to validate each element.
      error_handling: Whether and how to handle errors during iteration.
          If this is not set, invalid elements will fail the pipeline, otherwise
          invalid elements will be passed to the specified error output along
          with information about how the schema was invalidated.
  """
  def __init__(
      self,
      schema: Dict[str, Any],
      error_handling: Optional[Mapping[str, Any]] = None):
    self._schema = schema
    self._exception_handling_args = exception_handling_args(error_handling)

  @maybe_with_exception_handling
  def expand(self, pcoll):
    validator = json_utils.row_validator(
        schema_from_element_type(pcoll.element_type), self._schema)

    def invoke_validator(x):
      validator(x)
      return x

    return pcoll | beam.Map(invoke_validator)

  def with_exception_handling(self, **kwargs):
    # It's possible there's an error in iteration...
    self._exception_handling_args = kwargs
    return self


class _Explode(beam.PTransform):
  """Explodes (aka unnest/flatten) one or more fields producing multiple rows.

  Given one or more fields of iterable type, produces multiple rows, one for
  each value of that field. For example, a row of the form `('a', [1, 2, 3])`
  would expand to `('a', 1)`, `('a', 2')`, and `('a', 3)` when exploded on
  the second field.

  This is akin to a `FlatMap` when paired with the MapToFields transform.

  See more complete documentation on
  [YAML Mapping Functions](https://beam.apache.org/documentation/sdks/yaml-udf/#flatmap).

  Args:
      fields: The list of fields to expand.
      cross_product: If multiple fields are specified, indicates whether the
          full cross-product of combinations should be produced, or if the
          first element of the first field corresponds to the first element
          of the second field, etc. For example, the row
          `(['a', 'b'], [1, 2])` would expand to the four rows
          `('a', 1)`, `('a', 2)`, `('b', 1)`, and `('b', 2)` when
          `cross_product` is set to `true` but only the two rows
          `('a', 1)` and `('b', 2)` when it is set to `false`.
          Only meaningful (and required) if multiple rows are specified.
      error_handling: Whether and how to handle errors during iteration.
  """  # pylint: disable=line-too-long

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
  """Keeps only records that satisfy the given criteria.

  See more complete documentation on
  [YAML Filtering](https://beam.apache.org/documentation/sdks/yaml-udf/#filtering).
  """  # pylint: disable=line-too-long
  keep_fn = _as_callable_for_pcoll(pcoll, keep, "keep", language or 'generic')
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

  if append:
    return input_schema, {
        **{name: f'`{name}`' if language in ['sql', 'calcite'] else name
           for name in input_schema.keys() if name not in drop},
        **fields
    }
  else:
    return input_schema, fields


@beam.ptransform.ptransform_fn
@maybe_with_exception_handling_transform_fn
def _PyJsMapToFields(pcoll, language='generic', **mapping_args):
  """Creates records with new fields defined in terms of the input fields.

  See more complete documentation on
  [YAML Mapping Functions](https://beam.apache.org/documentation/sdks/yaml-udf/#mapping-functions).
  """  # pylint: disable=line-too-long
  input_schema, fields = normalize_fields(
      pcoll, language=language, **mapping_args)
  if language == 'javascript':
    options.YamlOptions.check_enabled(pcoll.pipeline, 'javascript')

  original_fields = list(input_schema.keys())

  return pcoll | beam.Select(
      **{
          name: _as_callable(
              original_fields, expr, name, language, input_schema)
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
      raise ValueError(f"Only expressions allowed in SQL at {name}.")

  selects = [
      f'({extract_expr(name, expr)}) AS `{name}`'
      for (name, expr) in fields.items()
  ]
  query = "SELECT " + ", ".join(selects) + " FROM PCOLLECTION"
  return pcoll | sql_transform_constructor(query)


@beam.ptransform.ptransform_fn
def _Partition(
    pcoll,
    by: Union[str, Dict[str, str]],
    outputs: List[str],
    unknown_output: Optional[str] = None,
    error_handling: Optional[Mapping[str, Any]] = None,
    language: Optional[str] = 'generic'):
  """Splits an input into several distinct outputs.

  Each input element will go to a distinct output based on the field or
  function given in the `by` configuration parameter.

  Args:
      by: A field, callable, or expression giving the destination output for
        this element.  Should return a string that is a member of the `outputs`
        parameter. If `unknown_output` is also set, other returns values are
        accepted as well, otherwise an error will be raised.
      outputs: The set of outputs into which this input is being partitioned.
      unknown_output: (Optional) If set, indicates a destination output for any
        elements that are not assigned an output listed in the `outputs`
        parameter.
      error_handling: (Optional) Whether and how to handle errors during
        partitioning.
      language: (Optional) The language of the `by` expression.
  """
  split_fn = _as_callable_for_pcoll(pcoll, by, 'by', language)
  try:
    split_fn_output_type = trivial_inference.infer_return_type(
        split_fn, [pcoll.element_type])
  except (TypeError, ValueError):
    pass
  else:
    if not typehints.is_consistent_with(split_fn_output_type,
                                        typehints.Optional[str]):
      raise ValueError(
          f'Partition function "{by}" must return a string type '
          f'not {split_fn_output_type}')
  error_output = error_handling['output'] if error_handling else None
  if error_output in outputs:
    raise ValueError(
        f'Error handling output "{error_output}" '
        f'cannot be among the listed outputs {outputs}')
  T = TypeVar('T')

  def split(element):
    tag = split_fn(element)
    if tag is None:
      tag = unknown_output
    if not isinstance(tag, str):
      raise ValueError(
          f'Returned output name "{tag}" of type {type(tag)} '
          f'from "{by}" must be a string.')
    if tag not in outputs:
      if unknown_output:
        tag = unknown_output
      else:
        raise ValueError(f'Unknown output name "{tag}" from {by}')
    return beam.pvalue.TaggedOutput(tag, element)

  output_set = set(outputs)
  if unknown_output:
    output_set.add(unknown_output)
  if error_output:
    output_set.add(error_output)
  mapping_transform = beam.Map(split)
  if error_output:
    mapping_transform = mapping_transform.with_exception_handling(
        **exception_handling_args(error_handling))
  else:
    mapping_transform = mapping_transform.with_outputs(*output_set)
  splits = pcoll | mapping_transform.with_input_types(T).with_output_types(T)
  result = {out: getattr(splits, out) for out in output_set}
  if error_output:
    result[error_output] = result[error_output] | map_errors_to_standard_format(
        pcoll.element_type)
  return result


@beam.ptransform.ptransform_fn
@maybe_with_exception_handling_transform_fn
def _AssignTimestamps(
    pcoll,
    timestamp: Union[str, Dict[str, str]],
    language: Optional[str] = None):
  """Assigns a new timestamp each element of its input.

  This can be useful when reading records that have the timestamp embedded
  in them, for example with various file types or other sources that by default
  set all timestamps to the infinite past.

  Note that the timestamp should only be set forward, as setting it backwards
  may not cause it to hold back an already advanced watermark and the data
  could become droppably late.

  Args:
      timestamp: A field, callable, or expression giving the new timestamp.
      language: The language of the timestamp expression.
      error_handling: Whether and how to handle errors during timestamp
        evaluation.
  """
  timestamp_fn = _as_callable_for_pcoll(pcoll, timestamp, 'timestamp', language)
  T = TypeVar('T')
  return pcoll | beam.Map(lambda x: TimestampedValue(x, timestamp_fn(x))
                          ).with_input_types(T).with_output_types(T)


def create_mapping_providers():
  # These are MetaInlineProviders because their expansion is in terms of other
  # YamlTransforms, but in a way that needs to be deferred until the input
  # schema is known.
  return [
      yaml_provider.InlineProvider({
          'AssignTimestamps-python': _AssignTimestamps,
          'AssignTimestamps-javascript': _AssignTimestamps,
          'AssignTimestamps-generic': _AssignTimestamps,
          'Explode': _Explode,
          'Filter-python': _PyJsFilter,
          'Filter-javascript': _PyJsFilter,
          'Filter-generic': _PyJsFilter,
          'MapToFields-python': _PyJsMapToFields,
          'MapToFields-javascript': _PyJsMapToFields,
          'MapToFields-generic': _PyJsMapToFields,
          'Partition-python': _Partition,
          'Partition-javascript': _Partition,
          'Partition-generic': _Partition,
          'StripErrorMetadata': _StripErrorMetadata,
          'ValidateWithSchema': _Validate,
      }),
      yaml_provider.SqlBackedProvider({
          'Filter-sql': _SqlFilterTransform,
          'Filter-calcite': _SqlFilterTransform,
          'MapToFields-sql': _SqlMapToFieldsTransform,
          'MapToFields-calcite': _SqlMapToFieldsTransform,
      }),
  ]
