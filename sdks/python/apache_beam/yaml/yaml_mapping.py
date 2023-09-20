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
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import Iterable
from typing import Mapping
from typing import Optional
from typing import Union

import js2py

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.typehints import row_type
from apache_beam.typehints import trivial_inference
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.utils import python_callable
from apache_beam.yaml import yaml_provider


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


# TODO(yaml) Consider adding optional language version parameter to support
#  ECMAScript 5 and 6
def _expand_javascript_mapping_func(
    original_fields, expression=None, callable=None, path=None, name=None):
  if expression:
    args = ', '.join(original_fields)
    js_func = f'function fn({args}) {{return ({expression})}}'
    js_callable = _CustomJsObjectWrapper(js2py.eval_js(js_func))
    return lambda __row__: js_callable(*__row__._asdict().values())

  elif callable:
    js_callable = _CustomJsObjectWrapper(js2py.eval_js(callable))
    return lambda __row__: js_callable(__row__._asdict())

  else:
    if not path.endswith('.js'):
      raise ValueError(f'File "{path}" is not a valid .js file.')
    udf_code = FileSystems.open(path).read().decode()
    js = js2py.EvalJs()
    js.eval(udf_code)
    js_callable = _CustomJsObjectWrapper(getattr(js, name))
    return lambda __row__: js_callable(__row__._asdict())


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


def _as_callable(original_fields, expr, transform_name, language):
  if expr in original_fields:
    return expr

  # TODO(yaml): support a type parameter
  # TODO(yaml): support an imports parameter
  # TODO(yaml): support a requirements parameter (possibly at a higher level)
  if isinstance(expr, str):
    expr = {'expression': expr}
  if not isinstance(expr, dict):
    raise ValueError(
        f"Ambiguous expression type (perhaps missing quoting?): {expr}")
  elif len(expr) != 1 and ('path' not in expr or 'name' not in expr):
    raise ValueError(f"Ambiguous expression type: {list(expr.keys())}")

  _check_mapping_arguments(transform_name, **expr)

  if language == "javascript":
    return _expand_javascript_mapping_func(original_fields, **expr)
  elif language == "python":
    return _expand_python_mapping_func(original_fields, **expr)
  else:
    raise ValueError(
        f'Unknown language for mapping transform: {language}. '
        'Supported languages are "javascript" and "python."')


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


# TODO(yaml): This should be available in all environments, in which case
# we choose the one that matches best.
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

    return (
        pcoll
        | beam.FlatMap(
            lambda row:
            (explode_cross_product if self._cross_product else explode_zip)
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

  input_schema = dict(named_fields_from_element_type(pcoll.element_type))
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
  except ValueError as exn:
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
  original_fields = list(input_schema.keys())

  return pcoll | beam.Select(
      **{
          name: _as_callable(original_fields, expr, name, language)
          for (name, expr) in fields.items()
      })


class SqlMappingProvider(yaml_provider.Provider):
  def __init__(self, sql_provider=None):
    if sql_provider is None:
      sql_provider = yaml_provider.beam_jar(
          urns={'Sql': 'beam:external:java:sql:v1'},
          gradle_target='sdks:java:extensions:sql:expansion-service:shadowJar')
    self._sql_provider = sql_provider

  def available(self):
    return self._sql_provider.available()

  def cache_artifacts(self):
    return self._sql_provider.cache_artifacts()

  def provided_transforms(self) -> Iterable[str]:
    return [
        'Filter-sql',
        'Filter-calcite',
        'MapToFields-sql',
        'MapToFields-calcite'
    ]

  def create_transform(
      self,
      typ: str,
      args: Mapping[str, Any],
      yaml_create_transform: Callable[
          [Mapping[str, Any], Iterable[beam.PCollection]], beam.PTransform]
  ) -> beam.PTransform:
    if typ.startswith('Filter-'):
      return _SqlFilterTransform(
          self._sql_provider, yaml_create_transform, **args)
    if typ.startswith('MapToFields-'):
      return _SqlMapToFieldsTransform(
          self._sql_provider, yaml_create_transform, **args)
    else:
      raise NotImplementedError(typ)

  def underlying_provider(self):
    return self._sql_provider

  def to_json(self):
    return {'type': "SqlMappingProvider"}


@beam.ptransform.ptransform_fn
def _SqlFilterTransform(
    pcoll, sql_provider, yaml_create_transform, keep, language):
  return pcoll | sql_provider.create_transform(
      'Sql', {'query': f'SELECT * FROM PCOLLECTION WHERE {keep}'},
      yaml_create_transform)


@beam.ptransform.ptransform_fn
def _SqlMapToFieldsTransform(
    pcoll, sql_provider, yaml_create_transform, **mapping_args):
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
  return pcoll | sql_provider.create_transform(
      'Sql', {'query': query}, yaml_create_transform)


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
      SqlMappingProvider(),
  ]
