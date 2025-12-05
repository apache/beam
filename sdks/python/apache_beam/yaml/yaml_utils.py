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

import json
import os
import re
import uuid
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any
from typing import Tuple

import yaml
from yaml import SafeLoader


class SafeLineLoader(SafeLoader):
  """A yaml loader that attaches line information to mappings and strings."""
  class TaggedString(str):
    """A string class to which we can attach metadata.

    This is primarily used to trace a string's origin back to its place in a
    yaml file.
    """
    def __reduce__(self):
      # Pickle as an ordinary string.
      return str, (str(self), )

  def construct_scalar(self, node):
    value = super().construct_scalar(node)
    if isinstance(value, str):
      value = SafeLineLoader.TaggedString(value)
      value._line_ = node.start_mark.line + 1
    return value

  def construct_mapping(self, node, deep=False):
    mapping = super().construct_mapping(node, deep=deep)
    mapping['__line__'] = node.start_mark.line + 1
    mapping['__uuid__'] = self.create_uuid()
    return mapping

  @classmethod
  def create_uuid(cls):
    return str(uuid.uuid4())

  @classmethod
  def strip_metadata(cls, spec, tagged_str=True):
    if isinstance(spec, Mapping):
      return {
          cls.strip_metadata(key, tagged_str): cls.strip_metadata(
              value, tagged_str)
          for (key, value) in spec.items()
          if key not in ('__line__', '__uuid__')
      }
    elif isinstance(spec, Iterable) and not isinstance(spec, (str, bytes)):
      return [cls.strip_metadata(value, tagged_str) for value in spec]
    elif isinstance(spec, SafeLineLoader.TaggedString) and tagged_str:
      return str(spec)
    else:
      return spec

  @staticmethod
  def get_line(obj):
    if isinstance(obj, dict):
      return obj.get('__line__', 'unknown')
    else:
      return getattr(obj, '_line_', 'unknown')


def patch_yaml(original_str: str, updated):
  """Updates a yaml string to match the updated with minimal changes.

  This only changes the portions of original_str that differ between
  original_str and updated in an attempt to preserve comments and formatting.
  """
  if not original_str and updated:
    return yaml.dump(updated, sort_keys=False)

  if original_str[-1] != '\n':
    # Add a trialing newline to avoid having to constantly check this edge case.
    # (It's also a good idea generally...)
    original_str += '\n'

  # The yaml parser returns positions in terms of line and column numbers.
  # Here we construct the mapping between the two.
  line_starts = [0]
  ix = original_str.find('\n')
  while ix != -1:
    line_starts.append(ix + 1)
    ix = original_str.find('\n', ix + 1)

  def pos(line_or_mark, column=0):
    if isinstance(line_or_mark, yaml.Mark):
      line = line_or_mark.line
      column = line_or_mark.column
    else:
      line = line_or_mark
    return line_starts[line] + column

  # Here we define a custom loader with hooks that record where each element is
  # found so we can swap it out appropriately.
  spans = {}

  class SafeMarkLoader(SafeLoader):
    pass

  # We create special subclass types to ensure each returned node is
  # a distinct object.
  marked_types = {}

  def record_yaml_scalar(constructor):
    def wrapper(self, node):
      raw_data = constructor(self, node)
      typ = type(raw_data)
      if typ not in marked_types:
        marked_types[typ] = type(f'Marked_{typ}', (type(raw_data), ), {})
      marked_data = marked_types[typ](raw_data)
      spans[id(marked_data)] = node.start_mark, node.end_mark
      return marked_data

    return wrapper

  SafeMarkLoader.add_constructor(
      'tag:yaml.org,2002:seq',
      record_yaml_scalar(SafeMarkLoader.construct_sequence))
  SafeMarkLoader.add_constructor(
      'tag:yaml.org,2002:map',
      record_yaml_scalar(SafeMarkLoader.construct_mapping))
  for typ in ('bool', 'int', 'float', 'binary', 'timestamp', 'str'):
    SafeMarkLoader.add_constructor(
        f'tag:yaml.org,2002:{typ}',
        record_yaml_scalar(getattr(SafeMarkLoader, f'construct_yaml_{typ}')))

  #  Now load the original yaml using our special parser.
  original = yaml.load(original_str, Loader=SafeMarkLoader)

  # This (recursively) finds the portion of the original string that must
  # be replaced with new content.
  def diff(a: Any, b: Any) -> Iterable[Tuple[int, int, str]]:
    if a == b:
      return
    elif (isinstance(a, dict) and isinstance(b, dict) and
          set(a.keys()) == set(b.keys()) and
          all(id(v) in spans for v in a.values())):
      for k, v in a.items():
        yield from diff(v, b[k])
    elif (isinstance(a, list) and isinstance(b, list) and a and b and
          all(id(v) in spans for v in a)):
      # Diff the matching entries.
      for va, vb in zip(a, b):
        yield from diff(va, vb)
      if len(b) < len(a):
        # Remove extra entries
        yield (
            # End of last preserved element.
            pos(spans[id(a[len(b) - 1])][1]),
            # End of last original element.
            pos(spans[id(a[-1])][1]),
            '')
      elif len(b) > len(a):
        # Add extra entries
        list_start, list_end = spans[id(a)]
        start_char = original_str[pos(list_start)]
        if start_char == '[':
          for v in b[len(a):]:
            yield pos(list_end) - 1, pos(list_end) - 1, ', ' + json.dumps(v)
        else:
          assert start_char == '-'
          indent = original_str[pos(list_start.line):pos(list_start)] + '- '
          content = original_str[pos(list_start):pos(list_end)].rstrip()
          actual_end_pos = pos(list_start) + len(content)
          for v in b[len(a):]:
            if isinstance(v, (list, dict)):
              v_str = (
                  yaml.dump(v, sort_keys=False)
                  # Indent.
                  .replace('\n', '\n' + ' ' * len(indent))
                  # Remove blank line indents.
                  .replace(' ' * len(indent) + '\n', '\n').rstrip())
            else:
              v_str = json.dumps(v)
            yield actual_end_pos, actual_end_pos, '\n' + indent + v_str

    else:
      start, end = spans[id(a)]
      indent = original_str[pos(start.line):pos(start)]
      # We strip trailing whitespace as the "end" of an element is often on
      # a subsequent line where the subsequent element actually starts.
      content = original_str[pos(start):pos(end)].rstrip()
      actual_end_pos = pos(start) + len(content)
      trailing = original_str[actual_end_pos:original_str.
                              find('\n', actual_end_pos)]
      if isinstance(b, (list, dict)):
        if indent.strip() in ('', '-') and not trailing.strip():
          # This element wholly occupies its set of lines, so it is safe to use
          # a multi-line yaml representation (appropriately indented).
          yield (
              pos(start),
              actual_end_pos,
              yaml.dump(b, sort_keys=False)
              # Indent.
              .replace('\n', '\n' + ' ' * len(indent))
              # Remove blank line indents.
              .replace(' ' * len(indent) + '\n', '\n').rstrip())
        else:
          # Force flow style.
          yield (
              pos(start),
              actual_end_pos,
              yaml.dump(b, default_flow_style=True, line_break=False).strip())
      elif isinstance(b, str) and re.match('^[A-Za-z0-9_]+$', b):
        # A simple string literal.
        yield pos(start), actual_end_pos, b
      else:
        # A scalar.
        yield pos(start), actual_end_pos, json.dumps(b)

  # Now stick it all together.
  last_end = 0
  content = []
  for start, end, new_content in sorted(diff(original, updated)):
    content.append(original_str[last_end:start])
    content.append(new_content)
    last_end = end
  content.append(original_str[last_end:])
  return ''.join(content)


def locate_data_file(relpath):
  return os.path.join(os.path.dirname(__file__), relpath)
