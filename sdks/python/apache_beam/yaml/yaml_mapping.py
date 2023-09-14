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


# TODO(yaml): This should be available in all environments, in which case
# we choose the one that matches best.
class _Explode(beam.PTransform):
  def __init__(self, fields, cross_product):
    self._fields = fields
    self._cross_product = cross_product
    self._exception_handling_args = None

  def expand(self, pcoll):
    all_fields = [
        x for x, _ in named_fields_from_element_type(pcoll.element_type)
    ]
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
        beam.core._MaybePValueWithErrors(pcoll, self._exception_handling_args)
        | beam.FlatMap(
            lambda row:
            (explode_cross_product if self._cross_product else explode_zip)
            ({name: getattr(row, name)
              for name in all_fields}, to_explode))).as_result()

  def infer_output_type(self, input_type):
    return row_type.RowTypeConstraint.from_fields([(
        name,
        trivial_inference.element_type(typ) if name in self._fields else
        typ) for (name, typ) in named_fields_from_element_type(input_type)])

  def with_exception_handling(self, **kwargs):
    # It's possible there's an error in iteration...
    self._exception_handling_args = kwargs
    return self


# TODO(yaml): Should Filter and Explode be distinct operations from Project?
# We'll want these per-language.
@beam.ptransform.ptransform_fn
def _PythonProjectionTransform(
    pcoll,
    *,
    fields,
    transform_name,
    language,
    keep=None,
    explode=(),
    cross_product=True,
    error_handling=None):
  original_fields = [
      name for (name, _) in named_fields_from_element_type(pcoll.element_type)
  ]

  if error_handling is None:
    error_handling_args = None
  else:
    error_handling_args = {
        'dead_letter_tag' if k == 'output' else k: v
        for (k, v) in error_handling.items()
    }

  pcoll = beam.core._MaybePValueWithErrors(pcoll, error_handling_args)

  if keep:
    if isinstance(keep, str) and keep in original_fields:
      keep_fn = lambda row: getattr(row, keep)
    else:
      keep_fn = _as_callable(original_fields, keep, transform_name, language)
    filtered = pcoll | beam.Filter(keep_fn)
  else:
    filtered = pcoll

  projected = filtered | beam.Select(
      **{
          name: _as_callable(original_fields, expr, transform_name, language)
          for (name, expr) in fields.items()
      })

  if explode:
    result = projected | _Explode(explode, cross_product=cross_product)
  else:
    result = projected

  return result.as_result(
      # TODO(https://github.com/apache/beam/issues/24755): Switch to MapTuple.
      beam.Map(
          lambda x: beam.Row(
              element=x[0], msg=str(x[1][1]), stack=str(x[1][2]))))


@beam.ptransform.ptransform_fn
def MapToFields(
    pcoll,
    yaml_create_transform,
    *,
    fields,
    keep=None,
    explode=(),
    cross_product=None,
    append=False,
    drop=(),
    language=None,
    error_handling=None,
    transform_name="MapToFields",
    **language_keywords):
  if isinstance(explode, str):
    explode = [explode]
  if cross_product is None:
    if len(explode) > 1:
      # TODO(robertwb): Consider if true is an OK default.
      raise ValueError(
          'cross_product must be specified true or false '
          'when exploding multiple fields')
    else:
      # Doesn't matter.
      cross_product = True

  input_schema = dict(named_fields_from_element_type(pcoll.element_type))
  if drop and not append:
    raise ValueError("Can only drop fields if append is true.")
  for name in drop:
    if name not in input_schema:
      raise ValueError(f'Dropping unknown field "{name}"')
  for name in explode:
    if not (name in fields or (append and name in input_schema)):
      raise ValueError(f'Exploding unknown field "{name}"')
  if append:
    for name in fields:
      if name in input_schema and name not in drop:
        raise ValueError(f'Redefinition of field "{name}"')

  if append:
    fields = {
        **{name: name
           for name in input_schema.keys() if name not in drop},
        **fields
    }

  if language is None:
    for name, expr in fields.items():
      if not isinstance(expr, str) or expr not in input_schema:
        # TODO(robertw): Could consider defaulting to SQL, or another
        # lowest-common-denominator expression language.
        raise ValueError("Missing language specification.")

    # We should support this for all languages.
    language = "python"

  if language in ("sql", "calcite"):
    if error_handling:
      raise ValueError('Error handling unsupported for sql.')
    selects = [f'{expr} AS {name}' for (name, expr) in fields.items()]
    query = "SELECT " + ", ".join(selects) + " FROM PCOLLECTION"
    if keep:
      query += " WHERE " + keep

    result = pcoll | yaml_create_transform({
        'type': 'Sql',
        'config': {
            'query': query, **language_keywords
        },
    }, [pcoll])
    if explode:
      # TODO(yaml): Implement via unnest.
      result = result | _Explode(explode, cross_product)

    return result

  elif language == 'python' or language == 'javascript':
    return pcoll | yaml_create_transform({
        'type': 'PyTransform',
        'config': {
            'constructor': __name__ + '._PythonProjectionTransform',
            'kwargs': {
                'fields': fields,
                'transform_name': transform_name,
                'language': language,
                'keep': keep,
                'explode': explode,
                'cross_product': cross_product,
                'error_handling': error_handling,
            },
            **language_keywords
        },
    }, [pcoll])

  else:
    # TODO(yaml): Support javascript expressions and UDFs.
    # TODO(yaml): Support java by fully qualified name.
    # TODO(yaml): Maybe support java lambdas?
    raise ValueError(
        f'Unknown language: {language}. '
        'Supported languages are "sql" (alias calcite) and "python."')


def create_mapping_provider():
  # These are MetaInlineProviders because their expansion is in terms of other
  # YamlTransforms, but in a way that needs to be deferred until the input
  # schema is known.
  return yaml_provider.MetaInlineProvider({
      'MapToFields': MapToFields,
      'Filter': (
          lambda yaml_create_transform,
          keep,
          **kwargs: MapToFields(
              yaml_create_transform,
              keep=keep,
              fields={},
              append=True,
              transform_name='Filter',
              **kwargs)),
      'Explode': (
          lambda yaml_create_transform,
          explode,
          **kwargs: MapToFields(
              yaml_create_transform,
              explode=explode,
              fields={},
              append=True,
              transform_name='Explode',
              **kwargs)),
  })
