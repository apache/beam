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

import sys
import threading

from future.utils import raise_


def timeout(timeout_secs):
  def decorate(fn):
    exc_info = []

    def wrapper(*args, **kwargs):
      def call_fn():
        try:
          fn(*args, **kwargs)
        except:  # pylint: disable=bare-except
          exc_info[:] = sys.exc_info()

      thread = threading.Thread(target=call_fn)
      thread.daemon = True
      thread.start()
      thread.join(timeout_secs)
      if exc_info:
        t, v, tb = exc_info  # pylint: disable=unbalanced-tuple-unpacking
        raise_(t, v, tb)
      assert not thread.is_alive(), 'timed out after %s seconds' % timeout_secs

    wrapper.__name__ = fn.__name__
    return wrapper

  return decorate
