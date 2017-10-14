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

# This module is experimental. No backwards-compatibility guarantees.


class StateSampler(object):

  def __init__(self, *args, **kwargs):
    pass

  def scoped_state(self, step_name, state_name=None, io_target=None):
    return _FakeScopedState()

  def start(self):
    pass

  def stop(self):
    pass

  def stop_if_still_running(self):
    self.stop()

  def commit_counters(self):
    pass


class _FakeScopedState(object):

  def __enter__(self):
    pass

  def __exit__(self, *unused_args):
    pass

  def sampled_seconds(self):
    return 0
