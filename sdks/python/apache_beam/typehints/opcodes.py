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

"""Defines the actions various bytecodes have on the frame.

Each function here corresponds to a bytecode documented in
https://docs.python.org/2/library/dis.html.  The first argument is a (mutable)
FrameState object, the second the integer opcode argument.

Bytecodes with more complicated behavior (e.g. modifying the program counter)
are handled inline rather than here.
"""
import types

from trivial_inference import union, element_type, Const, BoundMethod
import typehints
from typehints import Any, Dict, Iterable, List, Tuple, Union


def pop_one(state, unused_arg):
  del state.stack[-1:]


def pop_two(state, unused_arg):
  del state.stack[-2:]


def pop_three(state, unused_arg):
  del state.stack[-3:]


def push_value(v):

  def pusher(state, unused_arg):
    state.stack.append(v)

  return pusher


def nop(unused_state, unused_arg):
  pass


def pop_top(state, unused_arg):
  state.stack.pop()


def rot_n(state, n):
  state.stack[-n:] = [state.stack[-1]] + state.stack[-n:-1]


def rot_two(state, unused_arg):
  rot_n(state, 2)


def rot_three(state, unused_arg):
  rot_n(state, 3)


def rot_four(state, unused_arg):
  rot_n(state, 4)


def dup_top(state, unused_arg):
  state.stack.append(state.stack[-1])


def unary(state, unused_arg):
  state.stack[-1] = Const.unwrap(state.stack[-1])


unary_positive = unary_negative = unary_invert = unary


def unary_not(state, unused_arg):
  state.stack[-1] = bool


def unary_convert(state, unused_arg):
  state.stack[-1] = str


def get_iter(state, unused_arg):
  state.stack.append(Iterable[element_type(state.stack.pop())])


def symmetric_binary_op(state, unused_arg):
  # TODO(robertwb): This may not be entirely correct...
  b, a = state.stack.pop(), state.stack.pop()
  if a == b:
    state.stack.append(a)
  elif type(a) == type(b) and isinstance(a, typehints.SequenceTypeConstraint):
    state.stack.append(type(a)(union(element_type(a), element_type(b))))
  else:
    state.stack.append(Any)
# Except for int ** -int
binary_power = inplace_power = symmetric_binary_op
binary_multiply = inplace_multiply = symmetric_binary_op
binary_divide = inplace_divide = symmetric_binary_op
binary_floor_divide = inplace_floor_divide = symmetric_binary_op


def binary_true_divide(state, unused_arg):
  u = union(state.stack.pop(), state.stack.pop)
  if u == int:
    state.stack.append(float)
  else:
    state.stack.append(u)


inplace_true_divide = binary_true_divide

binary_modulo = inplace_modulo = symmetric_binary_op
# TODO(robertwb): Tuple add.
binary_add = inplace_add = symmetric_binary_op
binary_subtract = inplace_subtract = symmetric_binary_op


def binary_subscr(state, unused_arg):
  tos = state.stack.pop()
  if tos in (str, unicode):
    out = tos
  else:
    out = element_type(tos)
  state.stack.append(out)

# As far as types are concerned.
binary_lshift = inplace_lshift = binary_rshift = inplace_rshift = pop_top

binary_and = inplace_and = symmetric_binary_op
binary_xor = inplace_xor = symmetric_binary_op
binary_or = inpalce_or = symmetric_binary_op

# As far as types are concerned.
slice_0 = nop
slice_1 = slice_2 = pop_top
slice_3 = pop_two
store_slice_0 = store_slice_1 = store_slice_2 = store_slice_3 = nop
delete_slice_0 = delete_slice_1 = delete_slice_2 = delete_slice_3 = nop


def store_subscr(unused_state, unused_args):
  # TODO(robertwb): Update element/value type of iterable/dict.
  pass


binary_divide = binary_floor_divide = binary_modulo = symmetric_binary_op
binary_divide = binary_floor_divide = binary_modulo = symmetric_binary_op
binary_divide = binary_floor_divide = binary_modulo = symmetric_binary_op

# print_expr
print_item = pop_top
# print_item_to
print_newline = nop

# print_newline_to


# break_loop
# continue_loop
def list_append(state, arg):
  state.stack[-arg] = List[Union[element_type(state.stack[-arg]),
                                 Const.unwrap(state.stack.pop())]]


load_locals = push_value(Dict[str, Any])

# return_value
# yield_value
# import_star
exec_stmt = pop_three
# pop_block
# end_finally
build_class = pop_three

# setup_with
# with_cleanup


# store_name
# delete_name
def unpack_sequence(state, arg):
  t = state.stack.pop()
  if isinstance(t, Const):
    try:
      unpacked = [Const(ti) for ti in t.value]
      if len(unpacked) != arg:
        unpacked = [Any] * arg
    except TypeError:
      unpacked = [Any] * arg
  elif (isinstance(t, typehints.TupleHint.TupleConstraint)
        and len(t.tuple_types) == arg):
    unpacked = list(t.tuple_types)
  else:
    unpacked = [element_type(t)] * arg
  state.stack += reversed(unpacked)


def dup_topx(state, arg):
  state.stack += state[-arg:]


store_attr = pop_top
delete_attr = nop
store_global = pop_top
delete_global = nop


def load_const(state, arg):
  state.stack.append(state.const_type(arg))


load_name = push_value(Any)


def build_tuple(state, arg):
  if arg == 0:
    state.stack.append(Tuple[()])
  else:
    state.stack[-arg:] = [Tuple[[Const.unwrap(t) for t in state.stack[-arg:]]]]


def build_list(state, arg):
  if arg == 0:
    state.stack.append(List[Union[()]])
  else:
    state.stack[-arg:] = [List[reduce(union, state.stack[-arg:], Union[()])]]


build_map = push_value(Dict[Any, Any])


def load_attr(state, arg):
  o = state.stack.pop()
  name = state.get_name(arg)
  if isinstance(o, Const) and hasattr(o.value, name):
    state.stack.append(Const(getattr(o.value, name)))
  elif (isinstance(o, (type, types.ClassType))
        and isinstance(getattr(o, name, None), types.MethodType)):
    state.stack.append(Const(BoundMethod(getattr(o, name))))
  else:
    state.stack.append(Any)


def compare_op(state, unused_arg):
  # Could really be anything...
  state.stack[-2:] = [bool]


def import_name(state, unused_arg):
  state.stack[-2:] = [Any]


import_from = push_value(Any)

# jump

# for_iter


def load_global(state, arg):
  state.stack.append(state.get_global(arg))

# setup_loop
# setup_except
# setup_finally

store_map = pop_two


def load_fast(state, arg):
  state.stack.append(state.vars[arg])


def store_fast(state, arg):
  state.vars[arg] = state.stack.pop()


def delete_fast(state, arg):
  state.vars[arg] = Any  # really an error


def load_closure(state, unused_arg):
  state.stack.append(Any)  # really a Cell


def load_deref(state, arg):
  state.stack.append(state.closure_type(arg))
# raise_varargs


def call_function(state, arg, has_var=False, has_kw=False):
  # TODO(robertwb): Recognize builtins and dataflow objects
  # (especially special return values).
  pop_count = (arg & 0xF) + (arg & 0xF0) / 8 + 1 + has_var + has_kw
  state.stack[-pop_count:] = [Any]


def make_function(state, arg):
  state.stack[-arg - 1:] = [Any]  # a callable


def make_closure(state, arg):
  state.stack[-arg - 2:] = [Any]  # a callable


def build_slice(state, arg):
  state.stack[-arg:] = [Any]  # a slice object


def call_function_var(state, arg):
  call_function(state, arg, has_var=True)


def call_function_kw(state, arg):
  call_function(state, arg, has_kw=True)


def call_function_var_wk(state, arg):
  call_function(state, arg, has_var=True, has_kw=True)
