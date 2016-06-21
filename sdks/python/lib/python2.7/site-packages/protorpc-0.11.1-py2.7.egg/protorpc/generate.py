#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__author__ = 'rafek@google.com (Rafe Kaplan)'

import contextlib

from . import messages
from . import util

__all__ = ['IndentationError',
           'IndentWriter',
          ]


class IndentationError(messages.Error):
  """Raised when end_indent is called too many times."""


class IndentWriter(object):  
  """Utility class to make it easy to write formatted indented text.

  IndentWriter delegates to a file-like object and is able to keep track of the
  level of indentation.  Each call to write_line will write a line terminated
  by a new line proceeded by a number of spaces indicated by the current level
  of indentation.

  IndexWriter overloads the << operator to make line writing operations clearer.

  The indent method returns a context manager that can be used by the Python
  with statement that makes generating python code easier to use.  For example:

    index_writer << 'def factorial(n):'
    with index_writer.indent():
      index_writer << 'if n <= 1:'
      with index_writer.indent():
        index_writer << 'return 1'
      index_writer << 'else:'
      with index_writer.indent():
        index_writer << 'return factorial(n - 1)'

  This would generate:

  def factorial(n):
    if n <= 1:
      return 1
    else:
      return factorial(n - 1)
  """

  @util.positional(2)
  def __init__(self, output, indent_space=2):
    """Constructor.

    Args:
      output: File-like object to wrap.
      indent_space: Number of spaces each level of indentation will be.
    """
    # Private attributes:
    #
    #   __output: The wrapped file-like object.
    #   __indent_space: String to append for each level of indentation.
    #   __indentation: The current full indentation string.
    self.__output = output
    self.__indent_space = indent_space * ' '
    self.__indentation = 0

  @property
  def indent_level(self):
    """Current level of indentation for IndentWriter."""
    return self.__indentation

  def write_line(self, line):
    """Write line to wrapped file-like object using correct indentation.

    The line is written with the current level of indentation printed before it
    and terminated by a new line.

    Args:
      line: Line to write to wrapped file-like object.
    """
    if line != '':
      self.__output.write(self.__indentation * self.__indent_space)
      self.__output.write(line)
    self.__output.write('\n')

  def begin_indent(self):
    """Begin a level of indentation."""
    self.__indentation += 1

  def end_indent(self):
    """Undo the most recent level of indentation.

    Raises:
      IndentationError when called with no indentation levels.
    """
    if not self.__indentation:
      raise IndentationError('Unable to un-indent further')
    self.__indentation -= 1

  @contextlib.contextmanager
  def indent(self):
    """Create indentation level compatible with the Python 'with' keyword."""
    self.begin_indent()
    yield
    self.end_indent()

  def __lshift__(self, line):
    """Syntactic sugar for write_line method.

    Args:
      line: Line to write to wrapped file-like object.
    """
    self.write_line(line)
