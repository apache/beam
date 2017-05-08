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

"""DoFn Signature and method wrapper"""

from apache_beam.transforms.core import DoFn


class DoFnMethodWrapper(object):
  """Represents a method of a DoFn object."""

  def __init__(self, do_fn, method_name):
    """
    Initiates a ``DoFnMethodWrapper``.

    Args:
      do_fn: A DoFn object that contains the method.
      method_name: name of the method as a string.
    """

    args, _, _, defaults = do_fn.get_function_arguments(method_name)
    defaults = defaults if defaults else []
    method_value = getattr(do_fn, method_name)
    self.method_value = method_value
    self.args = args
    self.defaults = defaults


class DoFnSignature(object):
  """Represents the signature of a given ``DoFn`` object.

  Signature of a ``DoFn`` provides a view of the properties of a given ``DoFn``.
  Among other things, this will give an extensible way for for (1) accessing the
  structure of the ``DoFn`` including methods and method parameters
  (2) identifying features that a given ``DoFn`` support, for example, whether
  a given ``DoFn`` is a Splittable ``DoFn`` (
  https://s.apache.org/splittable-do-fn) (3) validating a ``DoFn`` based on the
  feature set offered by it.
  """

  def __init__(self, do_fn):
    assert isinstance(do_fn, DoFn)
    self.do_fn = do_fn

    # We add a property here for all methods defined by Beam DoFn features.
    self.process_method = DoFnMethodWrapper(do_fn, 'process')
    self.start_bundle_method = DoFnMethodWrapper(do_fn, 'start_bundle')
    self.finish_bundle_method = DoFnMethodWrapper(do_fn, 'finish_bundle')
    self._validate()

  def _validate(self):
    self._validate_process()
    self._validate_bundle_method(self.start_bundle_method)
    self._validate_bundle_method(self.finish_bundle_method)

  def _validate_process(self):
    for param in DoFn.DoFnParams:
      assert self.process_method.defaults.count(param) <= 1

  def _validate_bundle_method(self, method_wrapper):
    """Validate that none of the DoFnParameters are used in the function
    """
    for param in DoFn.DoFnParams:
      assert param not in method_wrapper.defaults
