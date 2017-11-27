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

"""The Python 3 disassembler source code backported to be compatible with
with Python 2.7.
"""

from __future__ import absolute_import
from __future__ import print_function

import collections
import dis
import sys


def get_instructions(x, first_line=None):
  """Iterator for the opcodes in methods, functions or code
  Generates a series of Instruction named tuples giving the details of
  each operations in the supplied code.
  If *first_line* is not None, it indicates the line number that should
  be reported for the first source line in the disassembled code.
  Otherwise, the source line information (if any) is taken directly from
  the disassembled code object.
  """
  co = _get_code_object(x)
  cell_names = co.co_cellvars + co.co_freevars
  linestarts = dict(dis.findlinestarts(co))
  if first_line is not None:
    line_offset = first_line - co.co_firstlineno
  else:
    line_offset = 0
  return _get_instructions_bytes(co.co_code, co.co_varnames, co.co_names,
                                 co.co_consts, cell_names, linestarts,
                                 line_offset)


def _get_code_object(x):
  """Helper to handle methods, functions, generators, strings
  and raw code objects"""
  if hasattr(x, '__func__'):  # Method
    x = x.__func__
  if hasattr(x, '__code__'):  # Function
    x = x.__code__
  if hasattr(x, 'gi_code'):  # Generator
    x = x.gi_code
  if isinstance(x, str):  # Source code
    x = _try_compile(x, "<disassembly>")
  if hasattr(x, 'co_code'):  # Code object
    return x
  raise TypeError("don't know how to disassemble %s objects" %
                  type(x).__name__)


def _try_compile(source, name):
  """Attempts to compile the given source, first as an expression and
     then as a statement if the first approach fails.
     Utility function to accept strings in functions that otherwise
     expect code objects
  """
  try:
    c = compile(source, name, 'eval')
  except SyntaxError:
    c = compile(source, name, 'exec')
  return c


def _get_instructions_bytes(code, varnames=None, names=None, constants=None,
                            cells=None, linestarts=None, line_offset=0):
  """Iterate over the instructions in a bytecode string.
  Generates a sequence of Instruction namedtuples giving the details of each
  opcode.  Additional information about the code's runtime environment
  (e.g. variable names, constants) can be specified using optional
  arguments.
  """
  labels = dis.findlabels(code)
  starts_line = None
  for offset, op, arg in _unpack_opargs(code):
    if linestarts is not None:
      starts_line = linestarts.get(offset, None)
      if starts_line is not None:
        starts_line += line_offset
    is_jump_target = offset in labels
    argval = None
    argrepr = ''
    if arg is not None:
      # Set argval to the dereferenced value of the argument when
      # available, and argrepr to the string representation of argval.
      # _disassemble_bytes needs the string repr of the
      # raw name index for LOAD_GLOBAL, LOAD_CONST, etc.
      argval = arg
      if op in dis.hasconst:
        argval, argrepr = _get_const_info(arg, constants)
      elif op in dis.hasname:
        argval, argrepr = _get_name_info(arg, names)
      elif op in dis.hasjrel:
        argval = offset + 3 + arg
        argrepr = "to " + repr(argval)
      elif op in dis.haslocal:
        argval, argrepr = _get_name_info(arg, varnames)
      elif op in dis.hascompare:
        argval = dis.cmp_op[arg]
        argrepr = argval
      elif op in dis.hasfree:
        argval, argrepr = _get_name_info(arg, cells)
      elif sys.version_info[0] == 3 and op in dis.hasnargs:
        argrepr = "%d positional, %d keyword pair" % (arg % 256, arg // 256)
    yield Instruction(dis.opname[op], op,
                      arg, argval, argrepr,
                      offset, starts_line, is_jump_target)


def _unpack_opargs(code):
  # enumerate() is not an option, since we sometimes process
  # multiple elements on a single pass through the loop
  extended_arg = 0
  n = len(code)
  i = 0
  while i < n:
    if sys.version_info[0] == 3:
      op = code[i]
    else:
      op = ord(code[i])
    offset = i
    i = i + 1
    arg = None
    if op >= dis.HAVE_ARGUMENT:
      if sys.version_info[0] == 3:
        arg = code[i] + code[i + 1] * 256 + extended_arg
      else:
        arg = ord(code[i]) + ord(code[i + 1]) * 256 + extended_arg
      extended_arg = 0
      i = i + 2
      if op == dis.EXTENDED_ARG:
        extended_arg = arg * 65536
    yield (offset, op, arg)


def _get_const_info(const_index, const_list):
  """Helper to get optional details about const references
     Returns the dereferenced constant and its repr if the constant
     list is defined.
     Otherwise returns the constant index and its repr().
  """
  argval = const_index
  if const_list is not None:
    argval = const_list[const_index]
  return argval, repr(argval)


def _get_name_info(name_index, name_list):
  """Helper to get optional details about named references
     Returns the dereferenced name as both value and repr if the name
     list is defined.
     Otherwise returns the name index and its repr().
  """
  argval = name_index
  if name_list is not None:
    argval = name_list[name_index]
    argrepr = argval
  else:
    argrepr = repr(argval)
  return argval, argrepr


_Instruction = collections.namedtuple(
    "_Instruction",
    "opname opcode arg argval argrepr offset starts_line is_jump_target")


class Instruction(_Instruction):
  """Details for a bytecode operation
     Defined fields:
       opname - human readable name for operation
       opcode - numeric code for operation
       arg - numeric argument to operation (if any), otherwise None
       argval - resolved arg value (if known), otherwise same as arg
       argrepr - human readable description of operation argument
       offset - start index of operation within bytecode sequence
       starts_line - line started by this opcode (if any), otherwise None
       is_jump_target - True if other code jumps to here, otherwise False
  """

  def _disassemble(self, lineno_width=3, mark_as_current=False):
    """Format instruction details for inclusion in disassembly output
    *lineno_width* sets the width of the line number field (0 omits it)
    *mark_as_current* inserts a '-->' marker arrow as part of the line
    """
    fields = []
    # Column: Source code line number
    if lineno_width:
      if self.starts_line is not None:
        lineno_fmt = "%%%dd" % lineno_width
        fields.append(lineno_fmt % self.starts_line)
      else:
        fields.append(' ' * lineno_width)
    # Column: Current instruction indicator
    if mark_as_current:
      fields.append('-->')
    else:
      fields.append('   ')
    # Column: Jump target marker
    if self.is_jump_target:
      fields.append('>>')
    else:
      fields.append('  ')
    # Column: Instruction offset from start of code sequence
    fields.append(repr(self.offset).rjust(4))
    # Column: Opcode name
    fields.append(self.opname.ljust(20))
    # Column: Opcode argument
    if self.arg is not None:
      fields.append(repr(self.arg).rjust(5))
      # Column: Opcode argument details
      if self.argrepr:
        fields.append('(' + self.argrepr + ')')
    return ' '.join(fields).rstrip()
