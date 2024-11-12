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

import argparse
import io
import itertools
import re

import docstring_parser
import yaml

from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import schemas
from apache_beam.utils import subprocess_server
from apache_beam.utils.python_callable import PythonCallableWithSource
from apache_beam.version import __version__ as beam_version
from apache_beam.yaml import json_utils
from apache_beam.yaml import yaml_provider
from apache_beam.yaml.yaml_errors import ErrorHandlingConfig


def _singular(name):
  # Simply removing an 's' (or 'es', or 'ies', ...) may result in surprising
  # manglings. Better to play it safe and leave a correctly-spelled plural
  # than a botched singular in our examples configs.
  return {
      'args': 'arg',
      'attributes': 'attribute',
      'elements': 'element',
      'fields': 'field',
  }.get(name, name)


def _fake_value(name, beam_type):
  type_info = beam_type.WhichOneof("type_info")
  if type_info == "atomic_type":
    if beam_type.atomic_type == schema_pb2.STRING:
      return f'"{name}"'
    elif beam_type.atomic_type == schema_pb2.BOOLEAN:
      return "true|false"
    else:
      return name
  elif type_info == "array_type":
    return [
        _fake_value(_singular(name), beam_type.array_type.element_type),
        _fake_value(_singular(name), beam_type.array_type.element_type),
        '...'
    ]
  elif type_info == "iterable_type":
    return [
        _fake_value(_singular(name), beam_type.iterable_type.element_type),
        _fake_value(_singular(name), beam_type.iterable_type.element_type),
        '...'
    ]
  elif type_info == "map_type":
    if beam_type.map_type.key_type.atomic_type == schema_pb2.STRING:
      return {
          'a': _fake_value(name + '_value_a', beam_type.map_type.value_type),
          'b': _fake_value(name + '_value_b', beam_type.map_type.value_type),
          'c': '...',
      }
    else:
      return {
          _fake_value(name + '_key', beam_type.map_type.key_type): _fake_value(
              name + '_value', beam_type.map_type.value_type)
      }
  elif type_info == "row_type":
    return _fake_row(beam_type.row_type.schema)
  elif type_info == "logical_type":
    return name
  else:
    raise ValueError(f"Unrecognized type_info: {type_info!r}")


def _fake_row(schema):
  if schema is None:
    return '...'
  return {f.name: _fake_value(f.name, f.type) for f in schema.fields}


def pretty_example(provider, t, base_t=None):
  spec = {'type': base_t or t}
  try:
    requires_inputs = provider.requires_inputs(t, {})
  except Exception:
    requires_inputs = False
  if requires_inputs:
    spec['input'] = '...'
  config_schema = provider.config_schema(t)
  if config_schema is None or config_schema.fields:
    spec['config'] = _fake_row(config_schema)
  s = yaml.dump(spec, sort_keys=False)
  return s.replace("'", "")


def config_docs(schema):
  if schema is None:
    return ''
  elif not schema.fields:
    return 'No configuration parameters.'

  def pretty_type(beam_type):
    type_info = beam_type.WhichOneof("type_info")
    if type_info == "atomic_type":
      return schema_pb2.AtomicType.Name(beam_type.atomic_type).lower()
    elif type_info == "array_type":
      return f'Array[{pretty_type(beam_type.array_type.element_type)}]'
    elif type_info == "iterable_type":
      return f'Iterable[{pretty_type(beam_type.iterable_type.element_type)}]'
    elif type_info == "map_type":
      return (
          f'Map[{pretty_type(beam_type.map_type.key_type)}, '
          f'{pretty_type(beam_type.map_type.value_type)}]')
    elif type_info == "row_type":
      return 'Row'
    else:
      return '?'

  def maybe_row_parameters(t):
    if t.WhichOneof("type_info") == "row_type":
      return indent('\n\nRow fields:\n\n' + config_docs(t.row_type.schema), 4)
    else:
      return ''

  def maybe_optional(t):
    return " (Optional)" if t.nullable else ""

  def normalize_error_handling(f):
    doc = docstring_parser.parse(
        ErrorHandlingConfig.__doc__, docstring_parser.DocstringStyle.GOOGLE)
    if f.name == "error_handling":
      f = schema_pb2.Field(
          name="error_handling",
          type=schema_pb2.FieldType(
              row_type=schema_pb2.RowType(
                  schema=schema_pb2.Schema(
                      fields=[
                          schemas.schema_field(
                              param.arg_name,
                              PythonCallableWithSource.load_from_expression(
                                  param.type_name),
                              param.description) for param in doc.params
                      ])),
              nullable=True),
          description=f.description or doc.short_description)
    return f

  def lines():
    for f in schema.fields:
      f = normalize_error_handling(f)
      yield ''.join([
          f'**{f.name}** `{pretty_type(f.type)}`',
          maybe_optional(f.type),
          indent(': ' + f.description if f.description else '', 2),
          maybe_row_parameters(f.type),
      ])

  return '\n\n'.join('*' + indent(line, 2) for line in lines()).strip()


def indent(lines, size):
  return '\n'.join(' ' * size + line for line in lines.split('\n'))


def longest(func, xs):
  return max([func(x) or '' for x in xs], key=len)


def io_grouping_key(transform_name):
  """Place reads and writes next to each other, after all other transforms."""
  if transform_name.startswith('ReadFrom'):
    return 1, transform_name[8:], 0
  elif transform_name.startswith('WriteTo'):
    return 1, transform_name[7:], 1
  else:
    return 0, transform_name


# Exclude providers
SKIP = {}


def add_transform_links(transform, description, provider_list):
  """
  Convert references of Providers to urls that link to their respective pages.
  Avoid self-linking within a Transform page.
  """
  for p in provider_list:
    description = re.sub(
        rf"(?<!type: )\b(?!{transform}\b)\b{p}\b",
        f'<a href="#{p.lower()}">{p}</a>',
        description or '')
  return description


def transform_docs(transform_base, transforms, providers, extra_docs=''):
  return '\n'.join([
      f'## {transform_base}',
      '',
      longest(
          lambda t: longest(
              lambda p: add_transform_links(
                  t, p.description(t), providers.keys()),
              providers[t]),
          transforms).replace('::\n', '\n\n    :::yaml\n'),
      '',
      extra_docs,
      '',
      '### Configuration',
      '',
      longest(
          lambda t: longest(
              lambda p: config_docs(p.config_schema(t)), providers[t]),
          transforms),
      '',
      '### Usage',
      '',
      '    :::yaml',
      '',
      indent(
          longest(
              lambda t: longest(
                  lambda p: pretty_example(p, t, transform_base), providers[t]),
              transforms),
          4),
  ])


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--markdown_file')
  parser.add_argument('--html_file')
  parser.add_argument('--schema_file')
  parser.add_argument('--include', default='.*')
  parser.add_argument('--exclude', default='')
  options = parser.parse_args()
  include = re.compile(options.include).match
  exclude = (
      re.compile(options.exclude).match
      if options.exclude else lambda x: x in SKIP)

  with subprocess_server.SubprocessServer.cache_subprocesses():
    json_config_schemas = []
    markdown_out = io.StringIO()
    providers = yaml_provider.standard_providers()
    for transform_base, transforms in itertools.groupby(
        sorted(providers.keys(), key=io_grouping_key),
        key=lambda s: s.split('-')[0]):
      transforms = list(transforms)
      if include(transform_base) and not exclude(transform_base):
        print(transform_base)
        if options.markdown_file or options.html_file:
          if '-' in transforms[0]:
            extra_docs = 'Supported languages: ' + ', '.join(
                t.split('-')[-1] for t in sorted(transforms)) + '.'
          else:
            extra_docs = ''
          markdown_out.write(
              transform_docs(transform_base, transforms, providers, extra_docs))
          markdown_out.write('\n\n')
        if options.schema_file:
          for transform in transforms:
            schema = providers[transform][0].config_schema(transform)
            if schema:
              json_config_schemas.append({
                  'if': {
                      'properties': {
                          'type': {
                              'const': transform
                          }
                      }
                  },
                  'then': {
                      'properties': {
                          'config': {
                              'type': 'object',
                              'properties': {
                                  '__line__': {
                                      'type': 'integer'
                                  },
                                  '__uuid__': {},
                                  **{
                                      f.name:  #
                                      json_utils.beam_type_to_json_type(f.type)
                                      for f in schema.fields
                                  }
                              },
                              'additionalProperties': False,
                          }
                      }
                  }
              })

    if options.schema_file:
      with open(options.schema_file, 'w') as fout:
        yaml.dump(json_config_schemas, fout, sort_keys=False)

    if options.markdown_file:
      with open(options.markdown_file, 'w') as fout:
        fout.write(markdown_out.getvalue())

    if options.html_file:
      import markdown
      import markdown.extensions.toc
      import pygments.formatters

      title = 'Beam YAML Transform Index'
      md = markdown.Markdown(
          extensions=[
              markdown.extensions.toc.TocExtension(toc_depth=2),
              'codehilite',
          ])
      pygments_style = pygments.formatters.HtmlFormatter().get_style_defs(
          '.codehilite')
      extra_style = '''
          * {
            box-sizing: border-box;
          }
          body {
            font-family: 'Roboto', sans-serif;
            font-weight: normal;
            color: #404040;
            background: #edf0f2;
          }
          .body-for-nav {
            background: #fcfcfc;
          }
          .grid-for-nav {
            width: 100%;
          }
          .nav-side {
            position: fixed;
            top: 0;
            left: 0;
            width: 300px;
            height: 100%;
            padding-bottom: 2em;
            color: #9b9b9b;
            background: #343131;
          }
          .nav-header {
            display: block;
            width: 300px;
            padding: 1em;
            background-color: #2980B9;
            text-align: center;
            color: #fcfcfc;
          }
          .nav-header a {
            color: #fcfcfc;
            font-weight: bold;
            display: inline-block;
            padding: 4px 6px;
            margin-bottom: 1em;
            text-decoration:none;
          }
          .nav-header>div.version {
            margin-top: -.5em;
            margin-bottom: 1em;
            font-weight: normal;
            color: rgba(255, 255, 255, 0.3);
          }
          .toc {
            width: 300px;
            text-align: left;
            overflow-y: auto;
            max-height: calc(100% - 4.3em);
            scrollbar-width: thin;
            scrollbar-color: #9b9b9b #343131;
          }
          .toc ul {
            margin: 0;
            padding: 0;
            list-style: none;
          }
          .toc li {
            border-bottom: 1px solid #4e4a4a;
            margin-left: 1em;
          }
          .toc a {
            display: block;
            line-height: 36px;
            font-size: 90%;
            color: #d9d9d9;
            padding: .1em 0.6em;
            text-decoration: none;
            transition: background-color 0.3s ease, color 0.3s ease;
          }
          .toc a:hover {
            background-color: #4e4a4a;
            color: #ffffff;
          }
          .transform-content-wrap {
            margin-left: 300px;
            background: #fcfcfc;
          }
          .transform-content {
            padding: 1.5em 3em;
            margin: 20px;
            padding-bottom: 2em;
          }
          .transform-content li::marker {
            display: inline-block;
            width: 0.5em;
          }
          .transform-content h1 {
            font-size: 40px;
          }
          .transform-content ul {
            margin-left: 0.75em;
            text-align: left;
            list-style-type: disc;
          }
          hr {
            color: gray;
            display: block;
            height: 1px;
            border: 0;
            border-top: 1px solid #e1e4e5;
            margin-bottom: 3em;
            margin-top: 3em;
            padding: 0;
          }
          .codehilite {
            background: #f5f5f5;
            border: 1px solid #ccc;
            border-radius: 4px;
            padding: 0.2em 1em;
            overflow: auto;
            font-family: monospace;
            font-size: 14px;
            line-height: 1.5;
          }
          p code, li code {
              white-space: nowrap;
              max-width: 100%;
              background: #fff;
              border: solid 1px #e1e4e5;
              padding: 0 5px;
              font-family: monospace;
              color: #404040;
              font-weight: bold;
              padding: 2px 5px;
          }
          '''

      html = md.convert(markdown_out.getvalue())
      with open(options.html_file, 'w') as html_out:
        html_out.write(
            f'''
            <html>
              <head>
                <title>{title}</title>
                <style>
                {pygments_style}
                {extra_style}
                </style>
              </head>
              <body class="body-for-nav">
                <div class="grid-for-nav">
                  <nav class="nav-side">
                    <div class="nav-header">
                      <a href=#>Beam YAML Transform Index</a>
                      <div class="version">
                        {beam_version}
                      </div>
                    </div>
                    {getattr(md, 'toc')}
                  </nav>
                  <section class="transform-content-wrap">
                    <div class="transform-content">
                      <h1>{title}</h1>
                      {html.replace('<h2', '<hr><h2')}
                    </div>
                  </section>
                </div>
              </body>
            </html>
            ''')


if __name__ == '__main__':
  main()
