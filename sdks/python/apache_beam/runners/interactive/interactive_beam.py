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

"""Module of Interactive Beam features that can be used in notebook.

The purpose of the module is to reduce the learning curve of Interactive Beam
users, provide a single place for importing and add sugar syntax for all
Interactive Beam components. It gives users capability to interact with existing
environment/session/context for Interactive Beam and visualize PCollections as
bounded dataset. In the meantime, it hides the interactivity implementation
from users so that users can focus on developing Beam pipeline without worrying
about how hidden states in the interactive session are managed.

Note: If you want backward-compatibility, only invoke interfaces provided by
this module in your notebook or application code.
"""

# pytype: skip-file

from __future__ import absolute_import

from apache_beam.runners.interactive import interactive_environment as ie


def watch(watchable):
  """Monitors a watchable.

  This allows Interactive Beam to implicitly pass on the information about the
  location of your pipeline definition.

  Current implementation mainly watches for PCollection variables defined in
  user code. A watchable can be a dictionary of variable metadata such as
  locals(), a str name of a module, a module object or an instance of a class.
  The variable can come from any scope even local variables in a method of a
  class defined in a module.

    Below are all valid::

      watch(__main__)  # if import __main__ is already invoked
      watch('__main__')  # does not require invoking import __main__ beforehand
      watch(self)  # inside a class
      watch(SomeInstance())  # an instance of a class
      watch(locals())  # inside a function, watching local variables within

  If you write a Beam pipeline in the __main__ module directly, since the
  __main__ module is always watched, you don't have to instruct Interactive
  Beam. If your Beam pipeline is defined in some module other than __main__,
  such as inside a class function or a unit test, you can watch() the scope.

    For example::

      class Foo(object)
        def run_pipeline(self):
          with beam.Pipeline() as p:
            init_pcoll = p |  'Init Create' >> beam.Create(range(10))
            watch(locals())
          return init_pcoll
      init_pcoll = Foo().run_pipeline()

    Interactive Beam caches init_pcoll for the first run.

    Then you can use::

      visualize(init_pcoll)

    To visualize data from init_pcoll once the pipeline is executed.
  """
  ie.current_env().watch(watchable)


def visualize(pcoll):
  """Visualizes a PCollection."""
  # TODO(BEAM-7926)
  pass
