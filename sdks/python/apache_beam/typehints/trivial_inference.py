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
"""
# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import collections
import dis
import inspect
import pprint
import sys
import traceback
import types
from builtins import object
from builtins import zip
from functools import reduce

from apache_beam import pvalue
from apache_beam.typehints import Any
from apache_beam.typehints import row_type
from apache_beam.typehints import typehints

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:  # Python 2
  import __builtin__ as builtins
except ImportError:  # Python 3
  import builtins  # type: ignore
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports


class TypeInferenceError(ValueError):
  """Error to raise when type inference failed."""
  pass


def instance_to_type(o):
  """Given a Python object o, return the corresponding type hint.
  """
  t = type(o)
  if o is None:
    return type(None)
  elif t == pvalue.Row:
    return row_type.RowTypeConstraint([
        (name, instance_to_type(value)) for name, value in o.as_dict().items()
    ])
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
    if len(o) > 0:
      return typehints.List[typehints.Union[[
          instance_to_type(item) for item in o
      ]]]
    else:
      return typehints.List[typehints.Any]
  elif t == set:
    if len(o) > 0:
      return typehints.Set[typehints.Union[[
          instance_to_type(item) for item in o
      ]]]
    else:
      return typehints.Set[typehints.Any]
  elif t == frozenset:
    if len(o) > 0:
      return typehints.FrozenSet[typehints.Union[[
          instance_to_type(item) for item in o
      ]]]
    else:
      return typehints.FrozenSet[typehints.Any]
  elif t == dict:
    if len(o) > 0:
      return typehints.Dict[
          typehints.Union[[instance_to_type(k) for k, v in o.items()]],
          typehints.Union[[instance_to_type(v) for k, v in o.items()]],
      ]
    else:
      return typehints.Dict[typehints.Any, typehints.Any]
  else:
    raise TypeInferenceError('Unknown forbidden type: %s' % t)


def union_list(xs, ys):
  assert len(xs) == len(ys)
  return [union(x, y) for x, y in zip(xs, ys)]


class Const(object):
  def __init__(self, value):
    self.value = value
    self.type = instance_to_type(value)

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
    if isinstance(val, typehints.TypeConstraint):
      return val
    else:
      return Const(val)

  def get_global(self, i):
    name = self.get_name(i)
    if name in self.f.__globals__:
      return Const(self.f.__globals__[name])
    if name in builtins.__dict__:
      return Const(builtins.__dict__[name])
    return Any

  def get_name(self, i):
    return self.co.co_names[i]

  def __repr__(self):
    return 'Stack: %s Vars: %s' % (self.stack, self.vars)

  def __or__(self, other):
    if self is None:
      return other.copy()
    elif other is None:
      return self.copy()
    return FrameState(
        self.f,
        union_list(self.vars, other.vars),
        union_list(self.stack, other.stack))

  def __ror__(self, left):
    return self | left


def union(a, b):
  """Returns the union of two types or Const values.
  """
  if a == b:
    return a
  elif not a:
    return b
  elif not b:
    return a
  a = Const.unwrap(a)
  b = Const.unwrap(b)
  # TODO(robertwb): Work this into the Union code in a more generic way.
  if type(a) == type(b) and element_type(a) == typehints.Union[()]:
    return b
  elif type(a) == type(b) and element_type(b) == typehints.Union[()]:
    return a
  return typehints.Union[a, b]


def finalize_hints(type_hint):
  """Sets type hint for empty data structures to Any."""
  def visitor(tc, unused_arg):
    if isinstance(tc, typehints.DictConstraint):
      empty_union = typehints.Union[()]
      if tc.key_type == empty_union:
        tc.key_type = Any
      if tc.value_type == empty_union:
        tc.value_type = Any

  if isinstance(type_hint, typehints.TypeConstraint):
    type_hint.visit(visitor, None)


def element_type(hint):
  """Returns the element type of a composite type.
  """
  hint = Const.unwrap(hint)
  if isinstance(hint, typehints.SequenceTypeConstraint):
    return hint.inner_type
  elif isinstance(hint, typehints.TupleHint.TupleConstraint):
    return typehints.Union[hint.tuple_types]
  return Any


def key_value_types(kv_type):
  """Returns the key and value type of a KV type.
  """
  # TODO(robertwb): Unions of tuples, etc.
  # TODO(robertwb): Assert?
  if (isinstance(kv_type, typehints.TupleHint.TupleConstraint) and
      len(kv_type.tuple_types) == 2):
    return kv_type.tuple_types
  return Any, Any


known_return_types = {
    len: int,
    hash: int,
}


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
            list: typehints.List[Any],
            set: typehints.Set[Any],
            frozenset: typehints.FrozenSet[Any],
            tuple: typehints.Tuple[Any, ...],
            dict: typehints.Dict[Any, Any]
        }[c]
      return c
    elif (c == getattr and len(input_types) == 2 and
          isinstance(input_types[1], Const)):
      from apache_beam.typehints import opcodes
      return opcodes._getattr(input_types[0], input_types[1].value)
    else:
      return Any
  except TypeInferenceError:
    if debug:
      traceback.print_exc()
    return Any
  except Exception:
    if debug:
      sys.stdout.flush()
      raise
    else:
      return Any


def infer_return_type_func(f, input_types, debug=False, depth=0):
  """Analyses a function to deduce its return type.

  Args:
    f: A Python function object to infer the return type of.
    input_types: A sequence of inputs corresponding to the input types.
    debug: Whether to print verbose debugging information.
    depth: Maximum inspection depth during type inference.

  Returns:
    A TypeConstraint that that the return value of this function will (likely)
    satisfy given the specified inputs.

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
  local_vars = list(input_types) + [typehints.Union[()]] * (
      len(co.co_varnames) - len(input_types))
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
          return_type = Any
        elif arg >> 8:
          if not var_args and not kw_args and not arg & 0xFF:
            # Keywords only, maybe it's a call to Row.
            if isinstance(state.stack[-pop_count], Const):
              from apache_beam.pvalue import Row
              if state.stack[-pop_count].value == Row:
                fields = state.stack[-pop_count + 1::2]
                types = state.stack[-pop_count + 2::2]
                return_type = row_type.RowTypeConstraint(
                    zip([fld.value for fld in fields], Const.unwrap_all(types)))
              else:
                return_type = Any
          else:
            # TODO(robertwb): Handle this case.
            return_type = Any
        elif isinstance(state.stack[-pop_count], Const):
          # TODO(robertwb): Handle this better.
          if var_args or kw_args:
            state.stack[-1] = Any
            state.stack[-var_args - kw_args] = Any
          return_type = infer_return_type(
              state.stack[-pop_count].value,
              state.stack[1 - pop_count:],
              debug=debug,
              depth=depth - 1)
        else:
          return_type = Any
        state.stack[-pop_count:] = [return_type]
      else:  # Python 3.6+
        if opname == 'CALL_FUNCTION':
          pop_count = arg + 1
          if depth <= 0:
            return_type = Any
          elif isinstance(state.stack[-pop_count], Const):
            return_type = infer_return_type(
                state.stack[-pop_count].value,
                state.stack[1 - pop_count:],
                debug=debug,
                depth=depth - 1)
          else:
            return_type = Any
        elif opname == 'CALL_FUNCTION_KW':
          # TODO(udim): Handle keyword arguments. Requires passing them by name
          #   to infer_return_type.
          pop_count = arg + 2
          if isinstance(state.stack[-pop_count], Const):
            from apache_beam.pvalue import Row
            if state.stack[-pop_count].value == Row:
              fields = state.stack[-1].value
              return_type = row_type.RowTypeConstraint(
                  zip(fields, Const.unwrap_all(state.stack[-pop_count + 1:-1])))
            else:
              return_type = Any
          else:
            return_type = Any
        elif opname == 'CALL_FUNCTION_EX':
          # stack[-has_kwargs]: Map of keyword args.
          # stack[-1 - has_kwargs]: Iterable of positional args.
          # stack[-2 - has_kwargs]: Function to call.
          has_kwargs = arg & 1  # type: int
          pop_count = has_kwargs + 2
          if has_kwargs:
            # TODO(udim): Unimplemented. Requires same functionality as a
            #   CALL_FUNCTION_KW implementation.
            return_type = Any
          else:
            args = state.stack[-1]
            _callable = state.stack[-2]
            if isinstance(args, typehints.ListConstraint):
              # Case where there's a single var_arg argument.
              args = [args]
            elif isinstance(args, typehints.TupleConstraint):
              args = list(args._inner_types())
            return_type = infer_return_type(
                _callable.value, args, debug=debug, depth=depth - 1)
        else:
          raise TypeInferenceError('unable to handle %s' % opname)
        state.stack[-pop_count:] = [return_type]
    elif opname == 'CALL_METHOD':
      pop_count = 1 + arg
      # LOAD_METHOD will return a non-Const (Any) if loading from an Any.
      if isinstance(state.stack[-pop_count], Const) and depth > 0:
        return_type = infer_return_type(
            state.stack[-pop_count].value,
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
      state.stack.append(element_type(state.stack[-1]))
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
    result = typehints.Iterable[reduce(union, Const.unwrap_all(yields))]
  else:
    result = reduce(union, Const.unwrap_all(returns))
  finalize_hints(result)

  if debug:
    print(f, id(f), input_types, '->', result)
  return result
