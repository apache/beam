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

"""Tests for protorpc.generate."""

__author__ = 'rafek@google.com (Rafe Kaplan)'

import operator

import cStringIO
import sys
import unittest

from protorpc import generate
from protorpc import test_util


class ModuleInterfaceTest(test_util.ModuleInterfaceTest,
                          test_util.TestCase):

  MODULE = generate


class IndentWriterTest(test_util.TestCase):

  def setUp(self):
    self.out = cStringIO.StringIO()
    self.indent_writer = generate.IndentWriter(self.out)

  def testWriteLine(self):
    self.indent_writer.write_line('This is a line')
    self.indent_writer.write_line('This is another line')

    self.assertEquals('This is a line\n'
                      'This is another line\n',
                      self.out.getvalue())

  def testLeftShift(self):
    self.run_count = 0
    def mock_write_line(line):
      self.run_count += 1
      self.assertEquals('same as calling write_line', line)

    self.indent_writer.write_line = mock_write_line
    self.indent_writer << 'same as calling write_line'
    self.assertEquals(1, self.run_count)

  def testIndentation(self):
    self.indent_writer << 'indent 0'
    self.indent_writer.begin_indent()
    self.indent_writer << 'indent 1'
    self.indent_writer.begin_indent()
    self.indent_writer << 'indent 2'
    self.indent_writer.end_indent()
    self.indent_writer << 'end 2'
    self.indent_writer.end_indent()
    self.indent_writer << 'end 1'
    self.assertRaises(generate.IndentationError,
                      self.indent_writer.end_indent)

    self.assertEquals('indent 0\n'
                      '  indent 1\n'
                      '    indent 2\n'
                      '  end 2\n'
                      'end 1\n',
                      self.out.getvalue())

  def testBlankLine(self):
    self.indent_writer << ''
    self.indent_writer.begin_indent()
    self.indent_writer << ''
    self.assertEquals('\n\n', self.out.getvalue())

  def testNoneInvalid(self):
    self.assertRaises(
      TypeError, operator.lshift, self.indent_writer, None)

  def testAltIndentation(self):
    self.indent_writer = generate.IndentWriter(self.out, indent_space=3)
    self.indent_writer << 'indent 0'
    self.assertEquals(0, self.indent_writer.indent_level)
    self.indent_writer.begin_indent()
    self.indent_writer << 'indent 1'
    self.assertEquals(1, self.indent_writer.indent_level)
    self.indent_writer.begin_indent()
    self.indent_writer << 'indent 2'
    self.assertEquals(2, self.indent_writer.indent_level)
    self.indent_writer.end_indent()
    self.indent_writer << 'end 2'
    self.assertEquals(1, self.indent_writer.indent_level)
    self.indent_writer.end_indent()
    self.indent_writer << 'end 1'
    self.assertEquals(0, self.indent_writer.indent_level)
    self.assertRaises(generate.IndentationError,
                      self.indent_writer.end_indent)
    self.assertEquals(0, self.indent_writer.indent_level)

    self.assertEquals('indent 0\n'
                      '   indent 1\n'
                      '      indent 2\n'
                      '   end 2\n'
                      'end 1\n',
                      self.out.getvalue())

  def testIndent(self):
    self.indent_writer << 'indent 0'
    self.assertEquals(0, self.indent_writer.indent_level)

    def indent1():
      self.indent_writer << 'indent 1'
      self.assertEquals(1, self.indent_writer.indent_level)

      def indent2():
        self.indent_writer << 'indent 2'
        self.assertEquals(2, self.indent_writer.indent_level)
      test_util.do_with(self.indent_writer.indent(), indent2)

      self.assertEquals(1, self.indent_writer.indent_level)
      self.indent_writer << 'end 2'
    test_util.do_with(self.indent_writer.indent(), indent1)

    self.assertEquals(0, self.indent_writer.indent_level)
    self.indent_writer << 'end 1'

    self.assertEquals('indent 0\n'
                      '  indent 1\n'
                      '    indent 2\n'
                      '  end 2\n'
                      'end 1\n',
                      self.out.getvalue())


def main():
  unittest.main()


if __name__ == '__main__':
  main()
