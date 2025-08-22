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

"""Customizations to how Python code objects are pickled.

This module provides helper functions to improve pickling code objects,
especially lambdas, in a consistent way by using code object identifiers. These
helper functions will be used to patch pickler implementations used by Beam
(e.g. Cloudpickle).

A code object identifier is a unique identifier for a code object that provides
a unique reference to the code object in the context where the code is defined
and is invariant to small changes in the surrounding code.

The code object identifiers consists of a sequence of the following parts
separated by periods:
- Module names - The name of the module the code object is in
- Class names - The name of a class containing the code object. There can be
  multiple of these in the same identifier in the case of nested
  classes.
- Function names - The name of the function containing the code object.
  There can be multiple of these in the case of nested functions.
- __code__ - Attribute indicating that we are entering the code object of a
  function/method.
- __co_consts__[<name>] - The name of the local variable containing the
  code object. In the case of lambdas, the name is created by using the
  signature of the lambda and hashing the bytecode, as shown below.

Examples:
- __main__.top_level_function.__code__
- __main__.ClassWithNestedFunction.process.__code__.co_consts[nested_function]
- __main__.ClassWithNestedLambda.process.__code__.co_consts[
    get_lambda_from_dictionary].co_consts[<lambda>, ('x',)]
- __main__.ClassWithNestedLambda.process.__code__.co_consts[
    <lambda>, ('x',), 1234567890]
"""

import collections
import hashlib
import inspect
import re
import sys
import types
from typing import Optional
from typing import Union


def get_normalized_path(path):
  """Returns a normalized path. This function is intended to be overridden."""
  return path


def get_code_object_identifier(callable: types.FunctionType):
  """Returns the code object identifier for a given callable.

  Args:
    callable: The callable object to search for.

  Returns:
    The code object identifier.
      Examples:
      - __main__.top_level_function.__code__
      - __main__.ClassWithNestedFunction.process.__code__.co_consts[
        nested_function]
      - __main__.ClassWithNestedLambda.process.__code__.co_consts[
        get_lambda_from_dictionary].co_consts[<lambda>, ('x',)]
      - __main__.ClassWithNestedLambda.process.__code__.co_consts[
        <lambda>, ('x',), 1234567890]
  """
  if not hasattr(callable, '__module__') or not hasattr(callable,
                                                        '__qualname__'):
    return None
  code_path: str = _extend_path(
      callable.__module__,
      _search(
          callable,
          sys.modules[callable.__module__],
          callable.__qualname__.split('.'),
      ),
  )
  return code_path


def _extend_path(prefix: str, current_path: Optional[str]):
  """Extends the path to the code object.

  Args:
    prefix: The prefix of the path.
    suffix: The rest of the path.

  Returns:
    The extended path.
  """
  if current_path is None:
    return None
  if not current_path:
    return prefix
  return prefix + '.' + current_path


def _search(
    callable: types.FunctionType,
    node: Union[types.ModuleType, types.FunctionType, types.CodeType],
    qual_name_parts: list[str]):
  """Searches an object to create a code object identifier.

  Recursively searches the tree of objects starting from node to find the
    callable's code object. It navigates through the attributes by using
    the first element of qual_name_parts to indicate what object it is
    currently at, then recursively passes through the rest of the list until
    the callable is found. Special components like '<locals>' and '<lambda>'
    direct the search within nested code objects.


  Example of qual_name_parts: ['MyClass', 'process', '<locals>', '<lambda>']

  Args:
    callable: The callable object to search for.
    node: The object to search within.
    qual_name_parts: A list of strings representing the qualified name of the
      callable object.

  Returns:
    The code object identifier, or None if not found.
  """
  if node is None:
    return None
  if not qual_name_parts:
    if (hasattr(node, '__code__') and hasattr(callable, '__code__') and
        node.__code__ == callable.__code__):
      return '__code__'
    else:
      return None
  if inspect.ismodule(node) or inspect.isclass(node):
    return _search_module_or_class(callable, node, qual_name_parts)
  elif inspect.isfunction(node):
    return _search_function(callable, node, qual_name_parts)
  elif inspect.iscode(node):
    return _search_code(callable, node, qual_name_parts)


def _search_module_or_class(
    callable: types.FunctionType,
    node: types.ModuleType,
    qual_name_parts: list[str]):
  """Searches a module or class to create a code object identifier.

  Args:
    callable: The callable object to search for.
    node: The module or class to search within.
    qual_name_parts: The list of qual name parts.

  Returns:
    The code object identifier, or None if not found.
  """
  # Functions/methods have a name that is unique within a given module or class
  # so the traversal can directly lookup function object identified by the name.
  # Lambdas don't have a name so we need to search all the attributes of the
  # node.
  first_part = qual_name_parts[0]
  rest = qual_name_parts[1:]
  if first_part == '<lambda>':
    for name in dir(node):
      value = getattr(node, name)
      if (hasattr(callable, '__code__') and
          isinstance(value, type(callable)) and
          value.__code__ == callable.__code__):
        return name + '.__code__'
      elif (isinstance(value, types.FunctionType) and
            value.__defaults__ is not None):
        # Python functions can have other functions as default parameters which
        # might contain the code object so we have to search them.
        for i, default_param_value in enumerate(value.__defaults__):
          path = _search(callable, default_param_value, rest)
          if path is not None:
            return _extend_path(name, _extend_path(f'__defaults__[{i}]', path))
  else:
    return _extend_path(
        first_part, _search(callable, getattr(node, first_part), rest))


def _search_function(
    callable: types.FunctionType,
    node: types.FunctionType,
    qual_name_parts: list[str]):
  """Searches a function to create a code object identifier.

  Args:
    callable: The callable object to search for.
    node: The function to search within.
    qual_name_parts: The list of qual name parts.

  Returns:
    The code object identifier, or None if not found.
  """
  first_part = qual_name_parts[0]
  if (node.__code__ == callable.__code__):
    if len(qual_name_parts) > 1:
      raise ValueError('Qual name parts too long')
    return '__code__'
  # If first part is '<locals>' then the code object is in a local variable
  # so we should add __code__ to the path to indicate that we are entering
  # the code object of the function.
  if first_part == '<locals>':
    return _extend_path(
        '__code__', _search(callable, node.__code__, qual_name_parts))


def _search_code(
    callable: types.FunctionType,
    node: types.CodeType,
    qual_name_parts: list[str]):
  """Searches a code object to create a code object identifier.

  Args:
    callable: The callable to search for.
    node: The code object to search within.
    qual_name_parts: The list of qual name parts.

  Returns:
    The code object identifier, or None if not found.

  Raises:
    ValueError: If the qual name parts are too long.
  """
  first_part = qual_name_parts[0]
  rest = qual_name_parts[1:]
  if hasattr(callable, '__code__') and node == callable.__code__:
    if len(qual_name_parts) > 1:
      raise ValueError('Qual name parts too long')
    return ''
  elif first_part == '<locals>':
    code_objects_by_name = collections.defaultdict(list)
    for co_const in node.co_consts:
      if inspect.iscode(co_const):
        code_objects_by_name[co_const.co_name].append(co_const)
    num_lambdas = len(code_objects_by_name.get('<lambda>', []))
    # If there is only one lambda, we can use the default path
    # 'co_consts[<lambda>]'. This is the most common case and it is
    # faster than calculating the signature and the hash.
    if num_lambdas == 1:
      path = _search(callable, code_objects_by_name['<lambda>'][0], rest)
      if path is not None:
        return _extend_path('co_consts[<lambda>]', path)
    else:
      return _search_lambda(callable, code_objects_by_name, rest)
  elif node.co_name == first_part:
    return _search(callable, node, rest)


def _search_lambda(
    callable: types.FunctionType,
    code_objects_by_name: dict[str, list[types.CodeType]],
    qual_name_parts: list[str]):
  """Searches a lambda to create a code object identifier.

  Args:
    callable: The callable to search for.
    code_objects_by_name: The code objects to search within, keyed by name.
    qual_name_parts: The rest of the qual_name_parts.

  Returns:
    The code object identifier, or None if not found.
  """
  # There are multiple lambdas in the code object, so we need to calculate
  # the signature and the hash to identify the correct lambda.
  lambda_code_objects_by_name = collections.defaultdict(list)
  name = qual_name_parts[0]
  code_objects = code_objects_by_name[name]
  if name == '<lambda>':
    for code_object in code_objects:
      lambda_name = f'<lambda>, {_signature(code_object)}'
      lambda_code_objects_by_name[lambda_name].append(code_object)
    # Check if there are any lambdas with the same signature.
    # If there are, we need to calculate the hash to identify the correct
    # lambda.
    for lambda_name, lambda_objects in lambda_code_objects_by_name.items():
      if len(lambda_objects) > 1:
        for lambda_object in lambda_objects:
          path = _search(callable, lambda_object, qual_name_parts)
          if path is not None:
            return _extend_path(
                f'co_consts[{lambda_name},'
                f' {_create_bytecode_hash(lambda_object)}]',
                path,
            )
      else:
        # If there is only one lambda with this signature, we can
        # use the signature to identify the correct lambda.
        path = _search(callable, code_objects[0], qual_name_parts)
        if path is not None:
          return _extend_path(f'co_consts[{lambda_name}]', path)
  else:
    # For non lambda objects, we can use the name to identify the object.
    path = _search(callable, code_objects[0], qual_name_parts)
    if path is not None:
      return _extend_path(f'co_consts[{name}]', path)


# Matches a path like: co_consts[my_function]
_SINGLE_NAME_PATTERN = re.compile(r'co_consts\[([a-zA-Z0-9\<\>_-]+)]')
# Matches a path like: co_consts[<lambda>, ('x',)]
_LAMBDA_WITH_ARGS_PATTERN = re.compile(
    r"co_consts\[(<[^>]+>),\s*(\('[^']*'\s*,\s*\))\]")
# Matches a path like: co_consts[<lambda>, ('x',), 1234567890]
_LAMBDA_WITH_HASH_PATTERN = re.compile(
    r"co_consts\[(<[^>]+>),\s*(\('[^']*'\s*,\s*\)),\s*(.+)\]")
# Matches a path like: __defaults__[0]
_DEFAULT_PATTERN = re.compile(r'(__defaults__)\[(\d+)\]')
# Matches an argument like: 'x'
_ARGUMENT_PATTERN = re.compile(r"'([^']*)'")


def _get_code_object_from_single_name_pattern(
    obj: types.ModuleType, name_result: re.Match[str], path: str):
  """Returns the code object from a name pattern.

  Args:
    obj: The object to search within.
    name_result: The result of the name pattern search.
    path: The path to the code object.

  Returns:
    The code object.

  Raises:
    ValueError: If the pattern is invalid.
    AttributeError: If the code object is not found.
  """
  if len(name_result.groups()) > 1:
    raise ValueError(f'Invalid pattern for single name: {name_result.group(0)}')
  # Groups are indexed starting at 1, group(0) is the entire match.
  name = name_result.group(1)
  for co_const in obj.co_consts:
    if inspect.iscode(co_const) and co_const.co_name == name:
      return co_const
  raise AttributeError(f'Could not find code object with path: {path}')


def _get_code_object_from_lambda_with_args_pattern(
    obj: types.ModuleType, lambda_with_args_result: re.Match[str], path: str):
  """Returns the code object from a lambda with args pattern.

  Args:
    obj: The object to search within.
    lambda_with_args_result: The result of the lambda with args pattern search.
    path: The path to the code object.

  Returns:
    The code object.

  Raises:
    AttributeError: If the code object is not found.
  """
  name = lambda_with_args_result.group(1)
  code_objects = collections.defaultdict(list)
  for co_const in obj.co_consts:
    if inspect.iscode(co_const) and co_const.co_name == name:
      code_objects[co_const.co_name].append(co_const)
  for name, objects in code_objects.items():
    for obj_ in objects:
      args = tuple(
          re.findall(_ARGUMENT_PATTERN, lambda_with_args_result.group(2)))
      if obj_.co_varnames == args:
        return obj_
  raise AttributeError(f'Could not find code object with path: {path}')


def _get_code_object_from_lambda_with_hash_pattern(
    obj: types.ModuleType, lambda_with_hash_result: re.Match[str], path: str):
  """Returns the code object from a lambda with hash pattern.

  Args:
    obj: The object to search within.
    lambda_with_hash_result: The result of the lambda with hash pattern search.
    path: The path to the code object.

  Returns:
    The code object.

  Raises:
    AttributeError: If the code object is not found.
  """
  name = lambda_with_hash_result.group(1)
  code_objects = collections.defaultdict(list)
  for co_const in obj.co_consts:
    if inspect.iscode(co_const) and co_const.co_name == name:
      code_objects[co_const.co_name].append(co_const)
  for name, objects in code_objects.items():
    for obj_ in objects:
      args = tuple(
          re.findall(_ARGUMENT_PATTERN, lambda_with_hash_result.group(2)))
      if obj_.co_varnames == args:
        hash_value = lambda_with_hash_result.group(3)
        if hash_value == str(_create_bytecode_hash(obj_)):
          return obj_
  raise AttributeError(f'Could not find code object with path: {path}')


def get_code_from_identifier(code_object_identifier: str):
  """Returns the code object corresponding to the code object identifier.

  Args:
    code_object_identifier: A string representing the code object identifier.

  Returns:
    The code object.

  Raises:
    ValueError: If the path is empty or invalid.
    AttributeError: If the attribute is not found.
  """
  if not code_object_identifier:
    raise ValueError('Path must not be empty.')
  parts = code_object_identifier.split('.')
  obj = sys.modules[parts[0]]
  for part in parts[1:]:
    if name_result := _SINGLE_NAME_PATTERN.fullmatch(part):
      obj = _get_code_object_from_single_name_pattern(
          obj, name_result, code_object_identifier)
    elif lambda_with_args_result := _LAMBDA_WITH_ARGS_PATTERN.fullmatch(part):
      obj = _get_code_object_from_lambda_with_args_pattern(
          obj, lambda_with_args_result, code_object_identifier)
    elif lambda_with_hash_result := _LAMBDA_WITH_HASH_PATTERN.fullmatch(part):
      obj = _get_code_object_from_lambda_with_hash_pattern(
          obj, lambda_with_hash_result, code_object_identifier)
    elif default_result := _DEFAULT_PATTERN.fullmatch(part):
      index = int(default_result.group(2))
      if index >= len(obj.__defaults__):
        raise ValueError(
            f'Index {index} is out of bounds for obj.__defaults__'
            f' {len(obj.__defaults__)} in path {code_object_identifier}')
      obj = getattr(obj, '__defaults__')[index]
    else:
      obj = getattr(obj, part)
  return obj


def _signature(obj: types.CodeType):
  """Returns the signature of a code object.

  The signature is the names of the arguments of the code object. This is used
  to unique identify lambdas.

  Args:
    obj: A code object, function, method, or cell.

  Returns:
    A tuple of the names of the arguments of the code object.
  """
  arg_count = (
      obj.co_argcount + obj.co_kwonlyargcount +
      (obj.co_flags & 4 == 4)  # PyCF_VARARGS
      + (obj.co_flags & 8 == 8)  # PyCF_VARKEYWORDS
  )
  return obj.co_varnames[:arg_count]


def _create_bytecode_hash(code_object: types.CodeType):
  """Returns the hash of a code object.

  Args:
    code_object: A code object.

  Returns:
    The hash of the code object.
  """
  return hashlib.md5(code_object.co_code).hexdigest()
