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

"""Python Callable utilities.

For internal use only; no backwards-compatibility guarantees.
"""

import importlib


class PythonCallableWithSource(object):
  """Represents a Python callable object with source codes before evaluated.

  Proxy object to Store a callable object with its string form (source code).
  The string form is used when the object is encoded and transferred to foreign
  SDKs (non-Python SDKs).

  Supported formats include fully-qualified names such as `math.sin`,
  expressions such as `lambda x: x * x` or `str.upper`, and multi-line function
  definitions such as `def foo(x): ...` or class definitions like
  `class Foo(...): ...`. If the source string contains multiple lines then lines
  prior to the last will be evaluated to provide the context in which to
  evaluate the expression, for example::

      import math

      lambda x: x - math.sin(x)

  is a valid chunk of source code.
  """
  def __init__(self, source: str) -> None:
    self._source = source
    self._callable = self.load_from_source(source)

  @classmethod
  def load_from_source(cls, source):
    if source in __builtins__:
      return cls.load_from_expression(source)
    elif all(s.isidentifier() for s in source.split('.')):
      if source.split('.')[0] in __builtins__:
        return cls.load_from_expression(source)
      else:
        return cls.load_from_fully_qualified_name(source)
    else:
      return cls.load_from_script(source)

  @staticmethod
  def load_from_expression(source):
    return eval(source)  # pylint: disable=eval-used

  @staticmethod
  def load_from_fully_qualified_name(fully_qualified_name):
    o = None
    path = ''
    for segment in fully_qualified_name.split('.'):
      path = '.'.join([path, segment]) if path else segment
      if o is not None and hasattr(o, segment):
        o = getattr(o, segment)
      else:
        o = importlib.import_module(path)
    return o

  @staticmethod
  def load_from_script(source, method_name=None):
    lines = [
        line for line in source.split('\n')
        if line.strip() and line.strip()[0] != '#'
    ]
    common_indent = min(len(line) - len(line.lstrip()) for line in lines)
    lines = [line[common_indent:] for line in lines]

    if method_name is None:
      for ix, line in reversed(list(enumerate(lines))):
        if line[0] != ' ':
          if line.startswith('def '):
            method_name = line[4:line.index('(')].strip()
          elif line.startswith('class '):
            method_name = line[5:line.index('(') if '(' in
                               line else line.index(':')].strip()
          else:
            method_name = '__python_callable__'
            lines[ix] = method_name + ' = ' + line
          break
      else:
        raise ValueError("Unable to identify callable from %r" % source)

    # pylint: disable=exec-used
    # pylint: disable=ungrouped-imports
    import apache_beam as beam
    exec_globals = {'beam': beam}
    exec('\n'.join(lines), exec_globals)
    return exec_globals[method_name]

  def default_label(self):
    src = self._source.strip()
    last_line = src.split('\n')[-1]
    if last_line[0] != ' ' and len(last_line) < 72:
      return last_line
    # Avoid circular import.
    from apache_beam.transforms.ptransform import label_from_callable
    return label_from_callable(self._callable)

  @property
  def _argspec_fn(self):
    return self._callable

  def get_source(self) -> str:
    return self._source

  def __call__(self, *args, **kwargs):
    return self._callable(*args, **kwargs)
