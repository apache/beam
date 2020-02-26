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

import re

import apache_beam as beam
from apache_beam.runners.interactive import background_caching_job as bcj
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive import pipeline_fragment as pf
from apache_beam.runners.interactive.display.pcoll_visualization import visualize
from apache_beam.runners.interactive.options import interactive_options


class Options(interactive_options.InteractiveOptions):
  """Options that guide how Interactive Beam works."""
  @property
  def enable_capture_replay(self):
    """Whether replayable source data capture should be replayed for multiple
    PCollection evaluations and pipeline runs as long as the data captured is
    still valid."""
    return self.capture_control._enable_capture_replay

  @enable_capture_replay.setter
  def enable_capture_replay(self, value):
    """Sets whether source data capture should be replayed. True - Enables
    capture of replayable source data so that following PCollection evaluations
    and pipeline runs always use the same data captured; False - Disables
    capture of replayable source data so that following PCollection evaluation
    and pipeline runs always use new data from sources."""
    self.capture_control._enable_capture_replay = value

  @property
  def capturable_sources(self):
    """Interactive Beam automatically captures data from sources in this set."""
    return self.capture_control._capturable_sources

  @property
  def capture_duration(self):
    """The data capture of sources ends as soon as the background caching job
    has run for this long."""
    return self.capture_control._capture_duration

  @capture_duration.setter
  def capture_duration(self, value):
    """Sets the capture duration as a timedelta.

    Example::

      # Sets the capture duration limit to 10 seconds.
      interactive_beam.options.capture_duration = timedelta(seconds=10)
      # Evicts all captured data if there is any.
      interactive_beam.evict_captured_data()
      # The next PCollection evaluation will capture fresh data from sources,
      # and the data captured will be replayed until another eviction.
    """
    self.capture_control._capture_duration = value

  # TODO(BEAM-8335): add capture_size options when they are supported.


# Users can set options to guide how Interactive Beam works.
# Examples:
# from datetime import timedelta
# from apache_beam.runners.interactive import interactive_beam as ib
# ib.options.enable_capture_replay = False/True
# ib.options.capture_duration = timedelta(seconds=60)
# ib.options.capturable_sources.add(SourceClass)
options = Options()


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


def show(*pcolls):
  """Visualizes given PCollections in an interactive exploratory way if used
  within a notebook, or prints a heading sampled data if used within an ipython
  shell. Noop if used in a non-interactive environment.

  Ad hoc builds a pipeline fragment including only transforms that are
  necessary to produce data for given PCollections pcolls, runs the pipeline
  fragment to compute data for those pcolls and then visualizes the data.

  The function is always blocking. If used within a notebook, the data
  visualized might be dynamically updated before the function returns as more
  and more data could getting processed and emitted when the pipeline fragment
  is being executed. If used within an ipython shell, there will be no dynamic
  plotting but a static plotting in the end of pipeline fragment execution.

  The PCollections given must belong to the same pipeline.

    For example::

      p = beam.Pipeline(InteractiveRunner())
      init = p | 'Init' >> beam.Create(range(1000))
      square = init | 'Square' >> beam.Map(lambda x: x * x)
      cube = init | 'Cube' >> beam.Map(lambda x: x ** 3)

      # Below builds a pipeline fragment from the defined pipeline `p` that
      # contains only applied transforms of `Init` and `Square`. Then the
      # interactive runner runs the pipeline fragment implicitly to compute data
      # represented by PCollection `square` and visualizes it.
      show(square)

      # This is equivalent to `show(square)` because `square` depends on `init`
      # and `init` is included in the pipeline fragment and computed anyway.
      show(init, square)

      # Below is similar to running `p.run()`. It computes data for both
      # PCollection `square` and PCollection `cube`, then visualizes them.
      show(square, cube)
  """
  assert len(pcolls) > 0, (
      'Need at least 1 PCollection to show data visualization.')
  for pcoll in pcolls:
    assert isinstance(pcoll, beam.pvalue.PCollection), (
        '{} is not an apache_beam.pvalue.PCollection.'.format(pcoll))
  user_pipeline = pcolls[0].pipeline
  for pcoll in pcolls:
    assert pcoll.pipeline is user_pipeline, (
        '{} belongs to a different user-defined pipeline ({}) than that of'
        ' other PCollections ({}).'.format(
            pcoll, pcoll.pipeline, user_pipeline))
  runner = user_pipeline.runner
  if isinstance(runner, ir.InteractiveRunner):
    runner = runner._underlying_runner

  # Make sure that all PCollections to be shown are watched. If a PCollection
  # has not been watched, make up a variable name for that PCollection and watch
  # it. No validation is needed here because the watch logic can handle
  # arbitrary variables.
  watched_pcollections = set()
  for watching in ie.current_env().watching():
    for _, val in watching:
      if hasattr(val, '__class__') and isinstance(val, beam.pvalue.PCollection):
        watched_pcollections.add(val)
  for pcoll in pcolls:
    if pcoll not in watched_pcollections:
      watch({re.sub(r'[\[\]\(\)]', '_', str(pcoll)): pcoll})

  # Attempt to run background caching job since we have the reference to the
  # user-defined pipeline.
  bcj.attempt_to_run_background_caching_job(runner, user_pipeline)

  # Build a pipeline fragment for the PCollections and run it.
  result = pf.PipelineFragment(list(pcolls)).run()
  ie.current_env().set_pipeline_result(user_pipeline, result)

  # If in notebook, dynamic plotting as computation goes.
  if ie.current_env().is_in_notebook:
    for pcoll in pcolls:
      visualize(pcoll, dynamic_plotting_interval=1)

  # Invoke wait_until_finish to ensure the blocking nature of this API without
  # relying on the run to be blocking.
  result.wait_until_finish()

  # If just in ipython shell, plotting once when the computation is completed.
  if ie.current_env().is_in_ipython and not ie.current_env().is_in_notebook:
    for pcoll in pcolls:
      visualize(pcoll)

  # If the pipeline execution is successful at this stage, mark the computation
  # completeness for the given PCollections so that when further `show`
  # invocation occurs, Interactive Beam wouldn't need to re-compute them.
  if result.state is beam.runners.runner.PipelineState.DONE:
    ie.current_env().mark_pcollection_computed(pcolls)


def evict_captured_data():
  """Forcefully evicts all captured replayable data.

  Once invoked, Interactive Beam will capture new data based on the guidance of
  options the next time it evaluates/visualizes PCollections or runs pipelines.
  """
  from apache_beam.runners.interactive.options import capture_control
  capture_control.evict_captured_data()
