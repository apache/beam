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

"""Defines the actions intrinsic bytecodes have on the frame.

Each function here corresponds to a bytecode documented in
https://docs.python.org/3/library/dis.html . The first argument is a (mutable)
FrameState object, the second the integer opcode argument.

Bytecodes with more complicated behavior (e.g. modifying the program counter)
are handled inline rather than here.

For internal use only; no backwards-compatibility guarantees.
"""
# pytype: skip-file

from . import opcodes


def intrinsic_1_invalid(state, arg):
  pass


def intrinsic_print(state, arg):
  pass


def intrinsic_import_star(state, arg):
  pass


def intrinsic_stopiteration_error(state, arg):
  pass


def intrinsic_async_gen_wrap(state, arg):
  pass


def intrinsic_unary_positive(state, arg):
  opcodes.unary_positive(state, arg)
  pass


def intrinsic_list_to_tuple(state, arg):
  opcodes.list_to_tuple(state, arg)
  pass


def intrinsic_typevar(state, arg):
  pass


def intrinsic_paramspec(state, arg):
  pass


def intrinsic_typevartuple(state, arg):
  pass


def intrinsic_subscript_generic(state, arg):
  pass


def intrinsic_typealias(state, arg):
  pass


# The order of operations in the table of the intrinsic one operations is
# defined in https://docs.python.org/3/library/dis.html#opcode-CALL_INTRINSIC_1
# and may change between minor versions.
INT_ONE_OPS = tuple([
    intrinsic_1_invalid,
    intrinsic_print,
    intrinsic_import_star,
    intrinsic_stopiteration_error,
    intrinsic_async_gen_wrap,
    intrinsic_unary_positive,
    intrinsic_list_to_tuple,
    intrinsic_typevar,
    intrinsic_paramspec,
    intrinsic_typevartuple,
    intrinsic_subscript_generic,
    intrinsic_typealias
])
