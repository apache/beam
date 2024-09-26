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

import threading


class ParentAwareThread(threading.Thread):
  """
  A thread subclass that is aware of its parent thread.
  
  This is useful in scenarios where work is executed in a child thread
  (e.g. ParDo#with_exception_handling(timeout)) and the child thread requires
  access to parent thread scoped state variables (e.g. state sampler).

  Attributes:
    parent_thread_id (int): The identifier of the parent thread that created
      this thread instance.
  """
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.parent_thread_id = threading.current_thread().ident
