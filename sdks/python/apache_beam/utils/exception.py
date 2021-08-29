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


def raise_exception(tp, value, tb):
  """
  Raise Exception from exc_info.
  Derived from `future.utils.raise_`
  """
  if value is not None and isinstance(tp, Exception):
    raise TypeError("instance exception may not have a separate value")
  if value is not None:
    exc = tp(value)
  else:
    exc = tp
  if exc.__traceback__ is not tb:
    raise exc.with_traceback(tb)
  raise exc
