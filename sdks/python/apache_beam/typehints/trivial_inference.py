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

"""Trivial type inference for simple functions.

For internal use only; no backwards-compatibility guarantees.

Empty is used to denote container type for which there is
no type information yet. For example a newly created list will be of type:
  Empty[typing.List].
If an int and str are appended to it it will become (through a series of
union()s):
  typing.List[int],
and then
  typing.List[typing.Union[int, str]].
The finalize_hints() function removes any
lingering Empties from the return value, e.g., if the inferred return value is:
  typing.Tuple[int, Empty[typing.List]]
then it will be converted to:
  typing.Tuple[int, typing.List[typing.Any]]
"""
from __future__ import absolute_import
from __future__ import print_function

import collections
import dis
import inspect
import pprint
import sys
import traceback
import types
import typing
from builtins import object
from builtins import zip
from functools import reduce

from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import convert_to_beam_type
from apache_beam.typehints.native_type_compatibility import convert_to_typing_types
from apache_beam.typehints.native_type_compatibility import get_args
from apache_beam.typehints.native_type_compatibility import is_Dict
from apache_beam.typehints.native_type_compatibility import is_Iterable
from apache_beam.typehints.native_type_compatibility import is_List
from apache_beam.typehints.native_type_compatibility import is_Set
from apache_beam.typehints.native_type_compatibility import is_Tuple
from apache_beam.typehints.native_type_compatibility import is_typing_type
from apache_beam.typehints.native_type_compatibility import same_container_type
from apache_beam.typehints.native_type_compatibility import set_args

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:                  # Python 2
  import __builtin__ as builtins
except ImportError:   # Python 3
  import builtins
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports


class TypeInferenceError(ValueError):
  """Error to raise when type inference failed."""
  pass


_empty = typing.TypeVar('_empty')


class Empty(typing.Generic[_empty]):
  """Placeholder for containers with yet-unknown element types.

  Will either be replaced with a specific type or Any (in finalize_hints).

  Based on pytypes.Empty. https://github.com/Stewori/pytypes

  Example: Empty[typing.List] denotes a list with unknown element type.
  """


def is_Empty(typ):
  """Checks if a given type is the special Empty type."""
  return getattr(typ, '__origin__', None) == Empty


def instance_to_type(o):
  """Given a Python object o, return the corresponding type hint.
  """
  t = type(o)
  if o is None:
    return type(None)
  elif t not in typehints.DISALLOWED_PRIMITIVE_TYPES:
    # pylint: disable=deprecated-types-field
    if sys.version_info[0] == 2 and t == types.InstanceType:
      return o.__class__
    if t == BoundMethod:
      return types.MethodType
    return t
  elif t == tuple:
    return typehints.Tuple[[instance_to_type(item) for item in o]]
  elif t == list:
    return typehints.List[
        typehints.Union[[instance_to_type(item) for item in o]]
    ]
  elif t == set:
    return typehints.Set[
        typehints.Union[[instance_to_type(item) for item in o]]
    ]
  elif t == dict:
    return typehints.Dict[
        typehints.Union[[instance_to_type(k) for k, v in o.items()]],
        typehints.Union[[instance_to_type(v) for k, v in o.items()]],
    ]
  else:
    raise TypeInferenceError('Unknown forbidden type: %s' % t)


def instance_to_native_type(o):
  """Given a Python object o, return the corresponding type hint.

  Returns:
    A Python ``type``, a ``typing`` module type, or ``Empty``. For
    composite types, any combination of these types.
  """
  t = type(o)
  if o is None:
    return type(None)
  if t not in typehints.DISALLOWED_PRIMITIVE_TYPES:
    # pylint: disable=deprecated-types-field
    if sys.version_info[0] == 2 and t == types.InstanceType:
      return o.__class__
    if t == BoundMethod:
      return types.MethodType
    return t

  if t == dict:
    if len(o):
      return typing.Dict[
          typing.Union[tuple(instance_to_native_type(k) for k, v in o.items())],
          typing.Union[tuple(instance_to_native_type(v) for k, v in o.items())],
      ]
    else:
      return Empty[typing.Dict]

  if t in [tuple, list, set]:
    typs = tuple(instance_to_native_type(item) for item in o)
    if t == tuple:
      return typing.Tuple[typs]
    elif t == list:
      if len(o):
        return typing.List[typing.Union[typs]]
      else:
        return Empty[typing.List]
    elif t == set:
      if len(o):
        return typing.Set[typing.Union[typs]]
      else:
        return Empty[typing.Set]

  raise TypeInferenceError('Unknown forbidden type: %s' % t)


def union_list(xs, ys):
  assert len(xs) == len(ys)
  return [union(x, y) for x, y in zip(xs, ys)]


class Const(object):

  def __init__(self, value):
    self.value = value
    self.type = instance_to_native_type(value)

  def __eq__(self, other):
    return isinstance(other, Const) and self.value == other.value

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash(self.value)

  def __repr__(self):
    return 'Const[%s]' % str(self.value)[:100]

  @staticmethod
  def unwrap(x):
    if isinstance(x, Const):
      return x.type
    return x

  @staticmethod
  def unwrap_all(xs):
    return [Const.unwrap(x) for x in xs]


class FrameState(object):
  """Stores the state of the frame at a particular point of execution.
  """

  def __init__(self, f, local_vars=None, stack=()):
    self.f = f
    self.co = f.__code__
    self.vars = list(local_vars)
    self.stack = list(stack)

  def __eq__(self, other):
    return isinstance(other, FrameState) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash(tuple(sorted(self.__dict__.items())))

  def copy(self):
    return FrameState(self.f, self.vars, self.stack)

  def const_type(self, i):
    return Const(self.co.co_consts[i])

  def get_closure(self, i):
    num_cellvars = len(self.co.co_cellvars)
    if i < num_cellvars:
      return self.vars[i]
    else:
      return self.f.__closure__[i - num_cellvars].cell_contents

  def closure_type(self, i):
    """Returns a TypeConstraint or Const."""
    val = self.get_closure(i)
    if is_typing_type(val):
      return val
    else:
      return Const(val)

  def get_global(self, i):
    name = self.get_name(i)
    if name in self.f.__globals__:
      return Const(self.f.__globals__[name])
    if name in builtins.__dict__:
      return Const(builtins.__dict__[name])
    return typing.Any

  def get_name(self, i):
    return self.co.co_names[i]

  def __repr__(self):
    return 'Stack: %s Vars: %s' % (self.stack, self.vars)

  def __or__(self, other):
    if self is None:
      return other.copy()
    elif other is None:
      return self.copy()
    return FrameState(self.f, union_list(self.vars, other.vars), union_list(
        self.stack, other.stack))

  def __ror__(self, left):
    return self | left


def union(a, b):
  """Returns the union of two types or Const values.

  If a is of type Empty then b is returned and vice versa.

  If a and b are of the same composite type T, return T[Union[args_a + args_b]].
  Not recursive, but could probably be made to be.
  """
  a = Const.unwrap(a)
  b = Const.unwrap(b)
  if a == b:
    return a
  elif not a:
    return b
  elif not b:
    return a
  if is_Empty(a) and same_container_type(get_args(a)[0], b):
    return b
  if is_Empty(b) and same_container_type(a, get_args(b)[0]):
    return a
  if is_Dict(a) and is_Dict(b):
    # Union keys and values separately.
    args_dict = tuple(typing.Union[args]
                      for args in zip(get_args(a), get_args(b)))
    return typing.Dict[args_dict]
  try:
    args = [arg for arg in get_args(a) + get_args(b) if arg is not Ellipsis]
    args_union = typing.Union[tuple(args)]
    if is_List(a) and is_List(b):
      return typing.List[args_union]
    if is_Tuple(a) and is_Tuple(b):
      return typing.Tuple[args_union, ...]
    if is_Set(a) and is_Set(b):
      return typing.Set[args_union]
    if is_Iterable(a) and is_Iterable(b):
      return typing.Iterable[args_union]
  except AttributeError:
    pass
  return typing.Union[a, b]


def finalize_hints(type_hint):
  """Converts Empty containers to containers of Any, recursively.

  Returns:
    A possibly new type hint.
  """
  if is_Empty(type_hint):
    inner_t = get_args(type_hint)[0]
    if inner_t == typing.Dict:
      return typing.Dict[typing.Any, typing.Any]
    if inner_t in [typing.List, typing.Set, typing.Iterable]:
      return inner_t[typing.Any]
  # Special representation of Empty[Tuple]: Tuple[()]
  if is_Tuple(type_hint) and get_args(type_hint) == ((), ):
    return typing.Tuple[typing.Any, ...]

  if is_typing_type(type_hint):
    try:
      args = get_args(type_hint)
      new_args = [finalize_hints(arg) for arg in args]
      # TODO(udim): Create a new hint instead of in-place modification hack.
      set_args(type_hint, tuple(new_args))
    except AttributeError:
      pass
  return type_hint


def element_type(hint):
  """Returns the element type of a composite type.

  Beam typing type version.
  """
  hint = Const.unwrap(hint)
  if isinstance(hint, typehints.SequenceTypeConstraint):
    return hint.inner_type
  elif isinstance(hint, typehints.TupleHint.TupleConstraint):
    return typehints.Union[hint.tuple_types]
  return typehints.Any


def element_typing_type(hint):
  """Returns the element type of a composite type.

  Python typing type version.
  """
  hint = Const.unwrap(hint)
  if is_Set(hint) or is_List(hint) or is_Iterable(hint):
    return get_args(hint)[0]
  if is_Tuple(hint):
    args = get_args(hint)
    if len(args) == 2 and args[1] == Ellipsis:
      return args[0]
    if len(args) == 1 and args[0] == ():
      return typing.Any
    return typing.Union[args]
  return typing.Any


def key_value_types(kv_type):
  """Returns the key and value type of a KV type.
  """
  # TODO(robertwb): Unions of tuples, etc.
  # TODO(robertwb): Assert?
  if (isinstance(kv_type, typehints.TupleHint.TupleConstraint)
      and len(kv_type.tuple_types) == 2):
    return kv_type.tuple_types
  return typehints.Any, typehints.Any


known_return_types = {len: int, hash: int,}


class BoundMethod(object):
  """Used to create a bound method when we only know the type of the instance.
  """

  def __init__(self, func, type):
    """Instantiates a bound method object.

    Args:
      func (types.FunctionType): The method's underlying function
      type (type): The class of the method.
    """
    self.func = func
    self.type = type


def hashable(c):
  try:
    hash(c)
    return True
  except TypeError:
    return False


def infer_return_type(c, input_types, debug=False, depth=5):
  """Analyses a callable to deduce its return type.

  Args:
    c: A Python callable to infer the return type of.
    input_types: A sequence of inputs corresponding to the input types.
    debug: Whether to print verbose debugging information.
    depth: Maximum inspection depth during type inference.

  Returns:
    A TypeConstraint that that the return value of this function will (likely)
    satisfy given the specified inputs.
  """
  # This is a wrapper that supports Beam typing types.
  return convert_to_beam_type(
      _infer_return_type(c, convert_to_typing_types(input_types), debug, depth),
      fail_on_unknown_typing_type=True)


def _infer_return_type(c, input_types, debug=False, depth=5):
  # This implementation uses Python-native typing module types.
  try:
    if hashable(c) and c in known_return_types:
      return known_return_types[c]
    elif isinstance(c, types.FunctionType):
      return infer_return_type_func(c, input_types, debug, depth)
    elif isinstance(c, types.MethodType):
      if c.__self__ is not None:
        input_types = [Const(c.__self__)] + input_types
      return infer_return_type_func(c.__func__, input_types, debug, depth)
    elif isinstance(c, BoundMethod):
      input_types = [c.type] + input_types
      return infer_return_type_func(c.func, input_types, debug, depth)
    elif inspect.isclass(c):
      if c in typehints.DISALLOWED_PRIMITIVE_TYPES:
        return {
            list: typing.List[typing.Any],
            set: typing.Set[typing.Any],
            tuple: typing.Tuple[typing.Any, ...],
            dict: typing.Dict[typing.Any, typing.Any]
        }[c]
      return c
    else:
      return typing.Any
  except TypeInferenceError:
    if debug:
      traceback.print_exc()
    return typing.Any
  except Exception:
    if debug:
      sys.stdout.flush()
      raise
    else:
      return typing.Any


def infer_return_type_func(f, input_types, debug=False, depth=0):
  """Analyses a function to deduce its return type.

  Args:
    f: A Python function object to infer the return type of.
    input_types: A sequence of inputs corresponding to the input types.
    debug: Whether to print verbose debugging information.
    depth: Maximum inspection depth during type inference.

  Returns:
    A typing module type that that the return value of this function will
    (likely) satisfy given the specified inputs.

  Raises:
    TypeInferenceError: if no type can be inferred.
  """
  if debug:
    print()
    print(f, id(f), input_types)
    dis.dis(f)
  from . import opcodes
  simple_ops = dict((k.upper(), v) for k, v in opcodes.__dict__.items())

  co = f.__code__
  code = co.co_code
  end = len(code)
  pc = 0
  extended_arg = 0  # Python 2 only.
  free = None

  yields = set()
  returns = set()
  # TODO(robertwb): Default args via inspect module.
  local_vars = list(input_types) + [typing.Any] * (len(co.co_varnames)
                                                   - len(input_types))
  state = FrameState(f, local_vars)
  states = collections.defaultdict(lambda: None)
  jumps = collections.defaultdict(int)

  # In Python 3, use dis library functions to disassemble bytecode and handle
  # EXTENDED_ARGs.
  is_py3 = sys.version_info[0] == 3
  if is_py3:
    ofs_table = {}  # offset -> instruction
    for instruction in dis.get_instructions(f):
      ofs_table[instruction.offset] = instruction

  # Python 2 - 3.5: 1 byte opcode + optional 2 byte arg (1 or 3 bytes).
  # Python 3.6+: 1 byte opcode + 1 byte arg (2 bytes, arg may be ignored).
  if sys.version_info >= (3, 6):
    inst_size = 2
    opt_arg_size = 0
  else:
    inst_size = 1
    opt_arg_size = 2

  last_pc = -1
  while pc < end:  # pylint: disable=too-many-nested-blocks
    start = pc
    if is_py3:
      instruction = ofs_table[pc]
      op = instruction.opcode
    else:
      op = ord(code[pc])
    if debug:
      print('-->' if pc == last_pc else '    ', end=' ')
      print(repr(pc).rjust(4), end=' ')
      print(dis.opname[op].ljust(20), end=' ')

    pc += inst_size
    if op >= dis.HAVE_ARGUMENT:
      if is_py3:
        arg = instruction.arg
      else:
        arg = ord(code[pc]) + ord(code[pc + 1]) * 256 + extended_arg
      extended_arg = 0
      pc += opt_arg_size
      if op == dis.EXTENDED_ARG:
        extended_arg = arg * 65536
      if debug:
        print(str(arg).rjust(5), end=' ')
        if op in dis.hasconst:
          print('(' + repr(co.co_consts[arg]) + ')', end=' ')
        elif op in dis.hasname:
          print('(' + co.co_names[arg] + ')', end=' ')
        elif op in dis.hasjrel:
          print('(to ' + repr(pc + arg) + ')', end=' ')
        elif op in dis.haslocal:
          print('(' + co.co_varnames[arg] + ')', end=' ')
        elif op in dis.hascompare:
          print('(' + dis.cmp_op[arg] + ')', end=' ')
        elif op in dis.hasfree:
          if free is None:
            free = co.co_cellvars + co.co_freevars
          print('(' + free[arg] + ')', end=' ')

    # Actually emulate the op.
    if state is None and states[start] is None:
      # No control reaches here (yet).
      if debug:
        print()
      continue
    state |= states[start]

    opname = dis.opname[op]
    jmp = jmp_state = None
    if opname.startswith('CALL_FUNCTION'):
      if sys.version_info < (3, 6):
        # Each keyword takes up two arguments on the stack (name and value).
        standard_args = (arg & 0xFF) + 2 * (arg >> 8)
        var_args = 'VAR' in opname
        kw_args = 'KW' in opname
        pop_count = standard_args + var_args + kw_args + 1
        if depth <= 0:
          return_type = typing.Any
        elif arg >> 8:
          # TODO(robertwb): Handle this case.
          return_type = typing.Any
        elif isinstance(state.stack[-pop_count], Const):
          # TODO(robertwb): Handle this better.
          if var_args or kw_args:
            state.stack[-1] = typing.Any
            state.stack[-var_args - kw_args] = typing.Any
          return_type = _infer_return_type(state.stack[-pop_count].value,
                                           state.stack[1 - pop_count:],
                                           debug=debug,
                                           depth=depth - 1)
        else:
          return_type = typing.Any
        state.stack[-pop_count:] = [return_type]
      else:  # Python 3.6+
        if opname == 'CALL_FUNCTION':
          pop_count = arg + 1
          if depth <= 0:
            return_type = typing.Any
          else:
            return_type = _infer_return_type(state.stack[-pop_count].value,
                                             state.stack[1 - pop_count:],
                                             debug=debug,
                                             depth=depth - 1)
        elif opname == 'CALL_FUNCTION_KW':
          # TODO(udim): Handle keyword arguments. Requires passing them by name
          #   to infer_return_type.
          pop_count = arg + 2
          return_type = typing.Any
        elif opname == 'CALL_FUNCTION_EX':
          # stack[-has_kwargs]: Map of keyword args.
          # stack[-1 - has_kwargs]: Iterable of positional args.
          # stack[-2 - has_kwargs]: Function to call.
          has_kwargs = arg & 1  # type: int
          pop_count = has_kwargs + 2
          if has_kwargs:
            # TODO(udim): Unimplemented. Requires same functionality as a
            #   CALL_FUNCTION_KW implementation.
            return_type = typing.Any
          else:
            args = state.stack[-1]
            _callable = state.stack[-2]
            return_type = None
            # Extract positional arguments, args should be an iterable object.
            if is_List(args):
              # Function args are provided in a List, which is unfortunate since
              # the number of args is unknown. Inference cannot continue here
              # (possible solution is to use a Const).
              return_type = typing.Any
            elif is_Tuple(args):
              args = get_args(args)
            else:
              # args has an unexpected type; this is an unhandled case.
              return_type = typing.Any
            if return_type is None:
              return_type = _infer_return_type(_callable.value,
                                               args,
                                               debug=debug,
                                               depth=depth - 1)
        else:
          raise TypeInferenceError('unable to handle %s' % opname)
        state.stack[-pop_count:] = [return_type]
    elif opname == 'CALL_METHOD':
      pop_count = 1 + arg
      # LOAD_METHOD will return a non-Const (Any) if loading from an Any.
      if isinstance(state.stack[-pop_count], Const) and depth > 0:
        return_type = _infer_return_type(state.stack[-pop_count].value,
                                         state.stack[1 - pop_count:],
                                         debug=debug,
                                         depth=depth - 1)
      else:
        return_type = typehints.Any
      state.stack[-pop_count:] = [return_type]
    elif opname in simple_ops:
      if debug:
        print("Executing simple op " + opname)
      simple_ops[opname](state, arg)
    elif opname == 'RETURN_VALUE':
      returns.add(state.stack[-1])
      state = None
    elif opname == 'YIELD_VALUE':
      yields.add(state.stack[-1])
    elif opname == 'JUMP_FORWARD':
      jmp = pc + arg
      jmp_state = state
      state = None
    elif opname == 'JUMP_ABSOLUTE':
      jmp = arg
      jmp_state = state
      state = None
    elif opname in ('POP_JUMP_IF_TRUE', 'POP_JUMP_IF_FALSE'):
      state.stack.pop()
      jmp = arg
      jmp_state = state.copy()
    elif opname in ('JUMP_IF_TRUE_OR_POP', 'JUMP_IF_FALSE_OR_POP'):
      jmp = arg
      jmp_state = state.copy()
      state.stack.pop()
    elif opname == 'FOR_ITER':
      jmp = pc + arg
      jmp_state = state.copy()
      jmp_state.stack.pop()
      state.stack.append(element_typing_type(state.stack[-1]))
    else:
      raise TypeInferenceError('unable to handle %s' % opname)

    if jmp is not None:
      # TODO(robertwb): Is this guaranteed to converge?
      new_state = states[jmp] | jmp_state
      if jmp < pc and new_state != states[jmp] and jumps[pc] < 5:
        jumps[pc] += 1
        pc = jmp
      states[jmp] = new_state

    if debug:
      print()
      print(state)
      pprint.pprint(dict(item for item in states.items() if item[1]))

  if yields:
    result = typing.Iterable[reduce(union, Const.unwrap_all(yields))]
  else:
    result = reduce(union, Const.unwrap_all(returns))
  result = finalize_hints(result)

  if debug:
    print(f, id(f), input_types, '->', result)
  return result
