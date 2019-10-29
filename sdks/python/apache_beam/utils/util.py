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

from __future__ import absolute_import

import functools


def memoize(func):
  cache = {}
  missing = object()

  @functools.wraps(func)
  def wrapper(*args):
    result = cache.get(args, missing)
    if result is missing:
      result = cache[args] = func(*args)
    return result
  return wrapper


def memoize_method(func):
  missing = object()

  @functools.wraps(func)
  def wrapper(self, *args):
    key = func, args
    try:
      cache = self.__memoize_method_cache
    except AttributeError:
      cache = self.__memoize_method_cache = {}
    result = cache.get(key, missing)
    if result is missing:
      result = cache[key] = func(self, *args)
    return result
  return wrapper


def only_element(iterable):
  element, = iterable
  return element
