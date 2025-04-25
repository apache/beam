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

import builtins
import collections
import dis
import inspect
import pprint
import sys
import traceback
import types
from functools import reduce

from apache_beam import pvalue
from apache_beam.typehints import Any
from apache_beam.typehints import row_type
from apache_beam.typehints import typehints
from apache_beam.utils import python_callable


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
    return row_type.RowTypeConstraint.from_fields([
        (name, instance_to_type(value)) for name, value in o.as_dict().items()
    ])
  elif t not in typehints.DISALLOWED_PRIMITIVE_TYPES:
    # pylint: disable=bad-option-value
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
  def __init__(self, f, local_vars=None, stack=(), kw_names=None):
    self.f = f
    self.co = f.__code__
    self.vars = list(local_vars)
    self.stack = list(stack)
    self.kw_names = kw_names

  def __eq__(self, other):
    return isinstance(other, FrameState) and self.__dict__ == other.__dict__

  def __hash__(self):
    return hash(tuple(sorted(self.__dict__.items())))

  def copy(self):
    return FrameState(self.f, self.vars, self.stack, self.kw_names)

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
  elif isinstance(hint,
                  typehints.UnionHint.UnionConstraint) and not hint.union_types:
    return hint
  return Any


def key_value_types(kv_type):
  """Returns the key and value type of a KV type.
  """
  # TODO(robertwb): Unions of tuples, etc.
  # TODO(robertwb): Assert?
  if (isinstance(kv_type, typehints.TupleHint.TupleConstraint) and
      len(kv_type.tuple_types) == 2):
    return kv_type.tuple_types
  elif isinstance(
      kv_type, typehints.UnionHint.UnionConstraint) and not kv_type.union_types:
    return kv_type, kv_type
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
    elif isinstance(c, python_callable.PythonCallableWithSource):
      # TODO(BEAM-24755): This can be removed once support for
      # inference across *args and **kwargs is implemented.
      return infer_return_type(c._callable, input_types, debug, depth)
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
    if (sys.version_info.major, sys.version_info.minor) >= (3, 11):
      dis.dis(f, show_caches=True)
    else:
      dis.dis(f)
  from . import opcodes
  simple_ops = dict((k.upper(), v) for k, v in opcodes.__dict__.items())
  from . import intrinsic_one_ops

  co = f.__code__
  code = co.co_code
  end = len(code)
  pc = 0
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
  ofs_table = {}  # offset -> instruction
  if (sys.version_info.major, sys.version_info.minor) >= (3, 11):
    dis_ints = dis.get_instructions(f, show_caches=True)
  else:
    dis_ints = dis.get_instructions(f)

  for instruction in dis_ints:
    ofs_table[instruction.offset] = instruction

  # Python 3.6+: 1 byte opcode + 1 byte arg (2 bytes, arg may be ignored).
  inst_size = 2
  opt_arg_size = 0

  # Python 3.10: bpo-27129 changes jump offsets to use instruction offsets,
  # not byte offsets. The offsets were halved (16 bits fro instructions vs 8
  # bits for bytes), so we have to double the value of arg.
  if (sys.version_info.major, sys.version_info.minor) >= (3, 10):
    jump_multiplier = 2
  else:
    jump_multiplier = 1

  last_pc = -1
  last_real_opname = opname = None
  while pc < end:  # pylint: disable=too-many-nested-blocks
    if opname not in ('PRECALL', 'CACHE'):
      last_real_opname = opname
    start = pc
    instruction = ofs_table[pc]
    op = instruction.opcode
    if debug:
      print('-->' if pc == last_pc else '    ', end=' ')
      print(repr(pc).rjust(4), end=' ')
      print(dis.opname[op].ljust(20), end=' ')

    pc += inst_size
    arg = None
    if op >= dis.HAVE_ARGUMENT:
      arg = instruction.arg
      pc += opt_arg_size
      if debug:
        print(str(arg).rjust(5), end=' ')
        if op in dis.hasconst:
          print('(' + repr(co.co_consts[arg]) + ')', end=' ')
        elif op in dis.hasname:
          if (sys.version_info.major, sys.version_info.minor) >= (3, 11):
            # Pre-emptively bit-shift so the print doesn't go out of index
            print_arg = arg >> 1
          else:
            print_arg = arg
          print('(' + co.co_names[print_arg] + ')', end=' ')
        elif op in dis.hasjrel:
          print('(to ' + repr(pc + (arg * jump_multiplier)) + ')', end=' ')
        elif op in dis.haslocal:
          print('(' + co.co_varnames[arg] + ')', end=' ')
        elif op in dis.hascompare:
          if (sys.version_info.major, sys.version_info.minor) >= (3, 12):
            # In 3.12 this arg was bit-shifted. Shifting it back avoids an
            # out-of-index.
            arg = arg >> 4
          print('(' + dis.cmp_op[arg] + ')', end=' ')
        elif op in dis.hasfree:
          if free is None:
            free = co.co_cellvars + co.co_freevars
          # From 3.11 on the arg is no longer offset by len(co_varnames)
          # so we adjust it back
          print_arg = arg
          if (sys.version_info.major, sys.version_info.minor) >= (3, 11):
            print_arg = arg - len(co.co_varnames)
          print('(' + free[print_arg] + ')', end=' ')

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
        # TODO(BEAM-24755): Handle keyword arguments. Requires passing them by
        #   name to infer_return_type.
        pop_count = arg + 2
        if isinstance(state.stack[-pop_count], Const):
          from apache_beam.pvalue import Row
          if state.stack[-pop_count].value == Row:
            fields = state.stack[-1].value
            return_type = row_type.RowTypeConstraint.from_fields(
                list(
                    zip(
                        fields,
                        Const.unwrap_all(state.stack[-pop_count + 1:-1]))))
          else:
            return_type = Any
        else:
          return_type = Any
      elif opname == 'CALL_FUNCTION_EX':
        # stack[-has_kwargs]: Map of keyword args.
        # stack[-1 - has_kwargs]: Iterable of positional args.
        # stack[-2 - has_kwargs]: Function to call.
        has_kwargs: int = arg & 1
        pop_count = has_kwargs + 2
        if has_kwargs:
          # TODO(BEAM-24755): Unimplemented. Requires same functionality as a
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
          elif isinstance(args, typehints.SequenceTypeConstraint):
            args = [element_type(args)] * len(
                inspect.getfullargspec(_callable.value).args)
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
    elif opname == 'CALL':
      pop_count = 1 + arg
      # Keyword Args case
      if state.kw_names is not None:
        if isinstance(state.stack[-pop_count], Const):
          from apache_beam.pvalue import Row
          if state.stack[-pop_count].value == Row:
            fields = state.kw_names
            return_type = row_type.RowTypeConstraint.from_fields(
                list(
                    zip(fields,
                        Const.unwrap_all(state.stack[-pop_count + 1:]))))
          else:
            return_type = Any
        state.kw_names = None
      else:
        # Handle comprehensions always having an arg of 0 for CALL
        # See https://github.com/python/cpython/issues/102403 for context.
        if (pop_count == 1 and last_real_opname == 'GET_ITER' and
            len(state.stack) > 1 and isinstance(state.stack[-2], Const) and
            getattr(state.stack[-2].value, '__name__', None) in (
                '<listcomp>', '<dictcomp>', '<setcomp>', '<genexpr>')):
          pop_count += 1
        if depth <= 0 or pop_count > len(state.stack):
          return_type = Any
        elif isinstance(state.stack[-pop_count], Const):
          return_type = infer_return_type(
              state.stack[-pop_count].value,
              state.stack[1 - pop_count:],
              debug=debug,
              depth=depth - 1)
        else:
          return_type = Any
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
      jmp = pc + arg * jump_multiplier
      jmp_state = state
      state = None
    elif opname in ('JUMP_BACKWARD', 'JUMP_BACKWARD_NO_INTERRUPT'):
      jmp = pc - (arg * jump_multiplier)
      jmp_state = state
      state = None
    elif opname == 'JUMP_ABSOLUTE':
      jmp = arg * jump_multiplier
      jmp_state = state
      state = None
    elif opname in ('POP_JUMP_IF_TRUE', 'POP_JUMP_IF_FALSE'):
      state.stack.pop()
      # The arg was changed to be a relative delta instead of an absolute
      # in 3.11, and became a full instruction instead of a
      # pseudo-instruction in 3.12
      if (sys.version_info.major, sys.version_info.minor) >= (3, 12):
        jmp = pc + arg * jump_multiplier
      else:
        jmp = arg * jump_multiplier
      jmp_state = state.copy()
    elif opname in ('POP_JUMP_FORWARD_IF_TRUE', 'POP_JUMP_FORWARD_IF_FALSE'):
      state.stack.pop()
      jmp = pc + arg * jump_multiplier
      jmp_state = state.copy()
    elif opname in ('POP_JUMP_BACKWARD_IF_TRUE', 'POP_JUMP_BACKWARD_IF_FALSE'):
      state.stack.pop()
      jmp = pc - (arg * jump_multiplier)
      jmp_state = state.copy()
    elif opname in ('POP_JUMP_FORWARD_IF_NONE', 'POP_JUMP_FORWARD_IF_NOT_NONE'):
      state.stack.pop()
      jmp = pc + arg * jump_multiplier
      jmp_state = state.copy()
    elif opname in ('POP_JUMP_BACKWARD_IF_NONE',
                    'POP_JUMP_BACKWARD_IF_NOT_NONE'):
      state.stack.pop()
      jmp = pc - (arg * jump_multiplier)
      jmp_state = state.copy()
    elif opname in ('JUMP_IF_TRUE_OR_POP', 'JUMP_IF_FALSE_OR_POP'):
      # The arg was changed to be a relative delta instead of an absolute
      # in 3.11
      if (sys.version_info.major, sys.version_info.minor) >= (3, 11):
        jmp = pc + arg * jump_multiplier
      else:
        jmp = arg * jump_multiplier
      jmp_state = state.copy()
      state.stack.pop()
    elif opname == 'FOR_ITER':
      jmp = pc + arg * jump_multiplier
      if sys.version_info >= (3, 12):
        # The jump is relative to the next instruction after a cache call,
        # so jump 4 more bytes.
        jmp += 4
      jmp_state = state.copy()
      jmp_state.stack.pop()
      state.stack.append(element_type(state.stack[-1]))
    elif opname == 'COPY_FREE_VARS':
      # Helps with calling closures, but since we aren't executing
      # them we can treat this as a no-op
      pass
    elif opname == 'KW_NAMES':
      tup = co.co_consts[arg]
      state.kw_names = tup
    elif opname == 'RESUME':
      # RESUME is a no-op
      pass
    elif opname == 'PUSH_NULL':
      # We're treating this as a no-op to avoid having to check
      # for extra None values on the stack when we extract return
      # values
      pass
    elif opname == 'PRECALL':
      # PRECALL is a no-op.
      pass
    elif opname == 'MAKE_CELL':
      # TODO: see if we need to implement cells like this
      pass
    elif opname == 'RETURN_GENERATOR':
      # TODO: see what this behavior is supposed to be beyond
      # putting something on the stack to be popped off
      state.stack.append(None)
      pass
    elif opname == 'CACHE':
      # No-op introduced in 3.11. Without handling this some
      # instructions have functionally > 2 byte size.
      pass
    elif opname == 'RETURN_CONST':
      # Introduced in 3.12. Handles returning constants directly
      # instead of having a LOAD_CONST before a RETURN_VALUE.
      returns.add(state.const_type(arg))
      state = None
    elif opname == 'CALL_INTRINSIC_1':
      # Introduced in 3.12. The arg is an index into a table of
      # operations reproduced in INT_ONE_OPS. Not all ops are
      # relevant for our type checking infrastructure.
      int_op = intrinsic_one_ops.INT_ONE_OPS[arg]
      if debug:
        print("Executing intrinsic one op", int_op.__name__.upper())
      int_op(state, arg)

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
