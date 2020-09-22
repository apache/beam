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

"""Module of the current Interactive Beam environment.

For internal use only; no backwards-compatibility guarantees.
Provides interfaces to interact with existing Interactive Beam environment.
External Interactive Beam users please use interactive_beam module in
application code or notebook.
"""
# pytype: skip-file

from __future__ import absolute_import

import atexit
import importlib
import logging
import os
import sys
import tempfile

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive.messaging.interactive_environment_inspector import InteractiveEnvironmentInspector
from apache_beam.runners.interactive.recording_manager import RecordingManager
from apache_beam.runners.interactive.utils import register_ipython_log_handler
from apache_beam.utils.interactive_utils import is_in_ipython
from apache_beam.utils.interactive_utils import is_in_notebook

# Interactive Beam user flow is data-centric rather than pipeline-centric, so
# there is only one global interactive environment instance that manages
# implementation that enables interactivity.
_interactive_beam_env = None

_LOGGER = logging.getLogger(__name__)

# By `format(customized_script=xxx)`, the given `customized_script` is
# guaranteed to be executed within access to a jquery with datatable plugin
# configured which is useful so that any `customized_script` is resilient to
# browser refresh. Inside `customized_script`, use `$` as jQuery.
_JQUERY_WITH_DATATABLE_TEMPLATE = """
        if (typeof window.interactive_beam_jquery == 'undefined') {{
          var jqueryScript = document.createElement('script');
          jqueryScript.src = 'https://code.jquery.com/jquery-3.4.1.slim.min.js';
          jqueryScript.type = 'text/javascript';
          jqueryScript.onload = function() {{
            var datatableScript = document.createElement('script');
            datatableScript.src = 'https://cdn.datatables.net/1.10.20/js/jquery.dataTables.min.js';
            datatableScript.type = 'text/javascript';
            datatableScript.onload = function() {{
              window.interactive_beam_jquery = jQuery.noConflict(true);
              window.interactive_beam_jquery(document).ready(function($){{
                {customized_script}
              }});
            }}
            document.head.appendChild(datatableScript);
          }};
          document.head.appendChild(jqueryScript);
        }} else {{
          window.interactive_beam_jquery(document).ready(function($){{
            {customized_script}
          }});
        }}"""

# By `format(hrefs=xxx)`, the given `hrefs` will be imported as HTML imports.
# Since HTML import might not be supported by the browser, we check if HTML
# import is supported by the browser, if so, import HTMLs else setup
# webcomponents and chain the HTML import to the end of onload.
_HTML_IMPORT_TEMPLATE = """
        var import_html = () => {{
          {hrefs}.forEach(href => {{
            var link = document.createElement('link');
            link.rel = 'import'
            link.href = href;
            document.head.appendChild(link);
          }});
        }}
        if ('import' in document.createElement('link')) {{
          import_html();
        }} else {{
          var webcomponentScript = document.createElement('script');
          webcomponentScript.src = 'https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js';
          webcomponentScript.type = 'text/javascript';
          webcomponentScript.onload = function(){{
            import_html();
          }};
          document.head.appendChild(webcomponentScript);
        }}"""


def current_env():
  """Gets current Interactive Beam environment."""
  global _interactive_beam_env
  if not _interactive_beam_env:
    _interactive_beam_env = InteractiveEnvironment()
  return _interactive_beam_env


def new_env():
  """Creates a new Interactive Beam environment to replace current one."""
  global _interactive_beam_env
  if _interactive_beam_env:
    _interactive_beam_env.cleanup()
  _interactive_beam_env = None
  return current_env()


class InteractiveEnvironment(object):
  """An interactive environment with cache and pipeline variable metadata.

  Interactive Beam will use the watched variable information to determine if a
  PCollection is assigned to a variable in user pipeline definition. When
  executing the pipeline, interactivity is applied with implicit cache
  mechanism for those PCollections if the pipeline is interactive. Users can
  also visualize and introspect those PCollections in user code since they have
  handles to the variables.
  """
  def __init__(self):
    # Registers a cleanup routine when system exits.
    atexit.register(self.cleanup)
    # Holds cache managers that manage source recording and intermediate
    # PCollection cache for each pipeline. Each key is a stringified user
    # defined pipeline instance's id.
    self._cache_managers = {}
    # Holds RecordingManagers keyed by pipeline instance id.
    self._recording_managers = {}
    # Holds class instances, module object, string of module names.
    self._watching_set = set()
    # Holds variables list of (Dict[str, object]).
    self._watching_dict_list = []
    # Holds results of main jobs as Dict[str, PipelineResult].
    # Each key is a pipeline instance defined by the end user. The
    # InteractiveRunner is responsible for populating this dictionary
    # implicitly.
    self._main_pipeline_results = {}
    # Holds background caching jobs as Dict[str, BackgroundCachingJob].
    # Each key is a pipeline instance defined by the end user. The
    # InteractiveRunner or its enclosing scope is responsible for populating
    # this dictionary implicitly when a background caching jobs is started.
    self._background_caching_jobs = {}
    # Holds TestStreamServiceControllers that controls gRPC servers serving
    # events as test stream of TestStreamPayload.Event.
    # Dict[str, TestStreamServiceController]. Each key is a pipeline
    # instance defined by the end user. The InteractiveRunner or its enclosing
    # scope is responsible for populating this dictionary implicitly when a new
    # controller is created to start a new gRPC server. The server stays alive
    # until a new background caching job is started thus invalidating everything
    # the gRPC server serves.
    self._test_stream_service_controllers = {}
    self._cached_source_signature = {}
    self._tracked_user_pipelines = set()
    # Tracks the computation completeness of PCollections. PCollections tracked
    # here don't need to be re-computed when data introspection is needed.
    self._computed_pcolls = set()
    # Always watch __main__ module.
    self.watch('__main__')
    # Do a warning level logging if current python version is below 3.6.
    if sys.version_info < (3, 6):
      self._is_py_version_ready = False
      _LOGGER.warning('Interactive Beam requires Python 3.5.3+.')
    else:
      self._is_py_version_ready = True
    # Check if [interactive] dependencies are installed.
    try:
      import IPython  # pylint: disable=unused-import
      import timeloop  # pylint: disable=unused-import
      from facets_overview.generic_feature_statistics_generator import GenericFeatureStatisticsGenerator  # pylint: disable=unused-import
      self._is_interactive_ready = True
    except ImportError:
      self._is_interactive_ready = False
      _LOGGER.warning(
          'Dependencies required for Interactive Beam PCollection '
          'visualization are not available, please use: `pip '
          'install apache-beam[interactive]` to install necessary '
          'dependencies to enable all data visualization features.')

    self._is_in_ipython = is_in_ipython()
    self._is_in_notebook = is_in_notebook()
    if not self._is_in_ipython:
      _LOGGER.warning(
          'You cannot use Interactive Beam features when you are '
          'not in an interactive environment such as a Jupyter '
          'notebook or ipython terminal.')
    if self._is_in_ipython and not self._is_in_notebook:
      _LOGGER.warning(
          'You have limited Interactive Beam features since your '
          'ipython kernel is not connected any notebook frontend.')
    if self._is_in_notebook:
      self.load_jquery_with_datatable()
      self.import_html_to_head([
          'https://raw.githubusercontent.com/PAIR-code/facets/1.0.0/facets-dist'
          '/facets-jupyter.html'
      ])
      register_ipython_log_handler()

    # A singleton inspector instance to message information of current
    # environment to other applications.
    self._inspector = InteractiveEnvironmentInspector()

  @property
  def options(self):
    """A reference to the global interactive options.

    Provided to avoid import loop or excessive dynamic import. All internal
    Interactive Beam modules should access interactive_beam.options through
    this property.
    """
    from apache_beam.runners.interactive.interactive_beam import options
    return options

  @property
  def is_py_version_ready(self):
    """If Python version is above the minimum requirement."""
    return self._is_py_version_ready

  @property
  def is_interactive_ready(self):
    """If the [interactive] dependencies are installed."""
    return self._is_interactive_ready

  @property
  def is_in_ipython(self):
    """If the runtime is within an IPython kernel."""
    return self._is_in_ipython

  @property
  def is_in_notebook(self):
    """If the kernel is connected to a notebook frontend.

    If not, it could be that the user is using kernel in a terminal or a unit
    test.
    """
    return self._is_in_notebook

  @property
  def inspector(self):
    """Gets the singleton InteractiveEnvironmentInspector to retrieve
    information consumable by other applications."""
    return self._inspector

  def cleanup(self, pipeline=None):
    """Cleans up cached states for the given pipeline. Noop if the given
    pipeline is absent from the environment. Cleans up for all pipelines
    if no pipeline is specified."""
    if pipeline:
      from apache_beam.runners.interactive import background_caching_job as bcj
      bcj.attempt_to_cancel_background_caching_job(pipeline)
      bcj.attempt_to_stop_test_stream_service(pipeline)
      cache_manager = self.get_cache_manager(pipeline)
      if cache_manager:
        cache_manager.cleanup()
    else:
      for _, job in self._background_caching_jobs.items():
        if job:
          job.cancel()
      for _, controller in self._test_stream_service_controllers.items():
        if controller:
          controller.stop()
      for _, cache_manager in self._cache_managers.items():
        if cache_manager:
          cache_manager.cleanup()

    self.evict_recording_manager(pipeline)
    self.evict_background_caching_job(pipeline)
    self.evict_test_stream_service_controller(pipeline)
    self.evict_computed_pcollections(pipeline)
    self.evict_cached_source_signature(pipeline)
    self.evict_pipeline_result(pipeline)

  def watch(self, watchable):
    """Watches a watchable.

    A watchable can be a dictionary of variable metadata such as locals(), a str
    name of a module, a module object or an instance of a class. The variable
    can come from any scope even local. Duplicated variable naming doesn't
    matter since they are different instances. Duplicated variables are also
    allowed when watching.
    """
    if isinstance(watchable, dict):
      self._watching_dict_list.append(watchable.items())
    else:
      self._watching_set.add(watchable)

  def watching(self):
    """Analyzes and returns a list of pair lists referring to variable names and
    values from watched scopes.

    Each entry in the list represents the variable defined within a watched
    watchable. Currently, each entry holds a list of pairs. The format might
    change in the future to hold more metadata. Duplicated pairs are allowed.
    And multiple paris can have the same variable name as the "first" while
    having different variable values as the "second" since variables in
    different scopes can have the same name.
    """
    watching = list(self._watching_dict_list)
    for watchable in self._watching_set:
      if isinstance(watchable, str):
        module = importlib.import_module(watchable)
        watching.append(vars(module).items())
      else:
        watching.append(vars(watchable).items())
    return watching

  def set_cache_manager(self, cache_manager, pipeline):
    """Sets the cache manager held by current Interactive Environment for the
    given pipeline."""
    if self.get_cache_manager(pipeline) is cache_manager:
      # NOOP if setting to the same cache_manager.
      return
    if self.get_cache_manager(pipeline):
      # Invoke cleanup routine when a new cache_manager is forcefully set and
      # current cache_manager is not None.
      self.cleanup(pipeline)
    self._cache_managers[str(id(pipeline))] = cache_manager

  def get_cache_manager(self, pipeline, create_if_absent=False):
    """Gets the cache manager held by current Interactive Environment for the
    given pipeline. If the pipeline is absent from the environment while
    create_if_absent is True, creates and returns a new file based cache
    manager for the pipeline."""
    cache_manager = self._cache_managers.get(str(id(pipeline)), None)
    if not cache_manager and create_if_absent:
      cache_dir = tempfile.mkdtemp(
          suffix=str(id(pipeline)),
          prefix='it-',
          dir=os.environ.get('TEST_TMPDIR', None))
      cache_manager = cache.FileBasedCacheManager(cache_dir)
      self._cache_managers[str(id(pipeline))] = cache_manager
    return cache_manager

  def evict_cache_manager(self, pipeline=None):
    """Evicts the cache manager held by current Interactive Environment for the
    given pipeline. Noop if the pipeline is absent from the environment. If no
    pipeline is specified, evicts for all pipelines."""
    self.cleanup(pipeline)
    if pipeline:
      return self._cache_managers.pop(str(id(pipeline)), None)
    self._cache_managers.clear()

  def set_recording_manager(self, recording_manager, pipeline):
    """Sets the recording manager for the given pipeline."""
    if self.get_recording_manager(pipeline) is recording_manager:
      # NOOP if setting to the same recording_manager.
      return
    self._recording_managers[str(id(pipeline))] = recording_manager

  def get_recording_manager(self, pipeline, create_if_absent=False):
    """Gets the recording manager for the given pipeline."""
    recording_manager = self._recording_managers.get(str(id(pipeline)), None)
    if not recording_manager and create_if_absent:
      # Get the pipeline variable name for the user. This is useful if the user
      # has multiple pipelines.
      pipeline_var = ''
      for w in self.watching():
        for var, val in w:
          if val is pipeline:
            pipeline_var = var
            break
      recording_manager = RecordingManager(pipeline, pipeline_var)
      self._recording_managers[str(id(pipeline))] = recording_manager
    return recording_manager

  def evict_recording_manager(self, pipeline):
    """Evicts the recording manager for the given pipeline.

    This stops the background caching job and clears the cache.
    Noop if the pipeline is absent from the environment. If no
    pipeline is specified, evicts for all pipelines.
    """
    if not pipeline:
      for rm in self._recording_managers.values():
        rm.cancel()
        rm.clear()
      self._recording_managers = {}
      return

    recording_manager = self.get_recording_manager(pipeline)
    if recording_manager:
      recording_manager.cancel()
      recording_manager.clear()
      del self._recording_managers[str(id(pipeline))]

  def describe_all_recordings(self):
    """Returns a description of the recording for all watched pipelnes."""
    return {
        self.pipeline_id_to_pipeline(pid): rm.describe()
        for pid,
        rm in self._recording_managers.items()
    }

  def set_pipeline_result(self, pipeline, result):
    """Sets the pipeline run result. Adds one if absent. Otherwise, replace."""
    assert issubclass(type(pipeline), beam.Pipeline), (
        'pipeline must be an instance of apache_beam.Pipeline or its subclass')
    assert issubclass(type(result), runner.PipelineResult), (
        'result must be an instance of '
        'apache_beam.runners.runner.PipelineResult or its subclass')
    self._main_pipeline_results[str(id(pipeline))] = result

  def evict_pipeline_result(self, pipeline=None):
    """Evicts the last run result of the given pipeline. Noop if the pipeline
    is absent from the environment. If no pipeline is specified, evicts for all
    pipelines."""
    if pipeline:
      return self._main_pipeline_results.pop(str(id(pipeline)), None)
    self._main_pipeline_results.clear()

  def pipeline_result(self, pipeline):
    """Gets the pipeline run result. None if absent."""
    return self._main_pipeline_results.get(str(id(pipeline)), None)

  def set_background_caching_job(self, pipeline, background_caching_job):
    """Sets the background caching job started from the given pipeline."""
    assert issubclass(type(pipeline), beam.Pipeline), (
        'pipeline must be an instance of apache_beam.Pipeline or its subclass')
    from apache_beam.runners.interactive.background_caching_job import BackgroundCachingJob
    assert isinstance(background_caching_job, BackgroundCachingJob), (
        'background_caching job must be an instance of BackgroundCachingJob')
    self._background_caching_jobs[str(id(pipeline))] = background_caching_job

  def get_background_caching_job(self, pipeline):
    """Gets the background caching job started from the given pipeline."""
    return self._background_caching_jobs.get(str(id(pipeline)), None)

  def evict_background_caching_job(self, pipeline=None):
    """Evicts the background caching job started from the given pipeline. Noop
    if the given pipeline is absent from the environment. If no pipeline is
    specified, evicts for all pipelines."""
    if pipeline:
      return self._background_caching_jobs.pop(str(id(pipeline)), None)
    self._background_caching_jobs.clear()

  def set_test_stream_service_controller(self, pipeline, controller):
    """Sets the test stream service controller that has started a gRPC server
    serving the test stream for any job started from the given user defined
    pipeline.
    """
    self._test_stream_service_controllers[str(id(pipeline))] = controller

  def get_test_stream_service_controller(self, pipeline):
    """Gets the test stream service controller that has started a gRPC server
    serving the test stream for any job started from the given user defined
    pipeline.
    """
    return self._test_stream_service_controllers.get(str(id(pipeline)), None)

  def evict_test_stream_service_controller(self, pipeline):
    """Evicts and pops the test stream service controller that has started a
    gRPC server serving the test stream for any job started from the given
    user defined pipeline. Noop if the given pipeline is absent from the
    environment. If no pipeline is specified, evicts for all pipelines.
    """
    if pipeline:
      return self._test_stream_service_controllers.pop(str(id(pipeline)), None)
    self._test_stream_service_controllers.clear()

  def is_terminated(self, pipeline):
    """Queries if the most recent job (by executing the given pipeline) state
    is in a terminal state. True if absent."""
    result = self.pipeline_result(pipeline)
    if result:
      return runner.PipelineState.is_terminal(result.state)
    return True

  def set_cached_source_signature(self, pipeline, signature):
    self._cached_source_signature[str(id(pipeline))] = signature

  def get_cached_source_signature(self, pipeline):
    return self._cached_source_signature.get(str(id(pipeline)), set())

  def evict_cached_source_signature(self, pipeline=None):
    """Evicts the signature generated for each recorded source of the given
    pipeline. Noop if the given pipeline is absent from the environment. If no
    pipeline is specified, evicts for all pipelines."""
    if pipeline:
      return self._cached_source_signature.pop(str(id(pipeline)), None)
    self._cached_source_signature.clear()

  def track_user_pipelines(self):
    """Record references to all user defined pipeline instances watched in
    current environment.

    Current static global singleton interactive environment holds references to
    a set of pipeline instances defined by the user in the watched scope.
    Interactive Beam features could use the references to determine if a given
    pipeline is defined by user or implicitly created by Beam SDK or runners,
    then handle them differently.

    This is invoked every time a PTransform is to be applied if the current
    code execution is under ipython due to the possibility that any user defined
    pipeline can be re-evaluated through notebook cell re-execution at any time.

    Each time this is invoked, it will check if there is a cache manager
    already created for each user defined pipeline. If not, create one for it.

    If a pipeline is no longer watched due to re-execution while its
    PCollections are still in watched scope, the pipeline becomes anonymous but
    still accessible indirectly through references to its PCollections. This
    function also clears up internal states for those anonymous pipelines once
    all their PCollections are anonymous.
    """
    self._tracked_user_pipelines = set()
    for watching in self.watching():
      for _, val in watching:
        if isinstance(val, beam.pipeline.Pipeline):
          self._tracked_user_pipelines.add(val)
          _ = self.get_cache_manager(val, create_if_absent=True)
          _ = self.get_recording_manager(val, create_if_absent=True)
    all_tracked_pipeline_ids = set(self._background_caching_jobs.keys()).union(
        set(self._test_stream_service_controllers.keys()),
        set(self._cache_managers.keys()),
        {str(id(pcoll.pipeline))
         for pcoll in self._computed_pcolls},
        set(self._cached_source_signature.keys()),
        set(self._main_pipeline_results.keys()))
    inspectable_pipelines = self._inspector.inspectable_pipelines
    for pipeline in all_tracked_pipeline_ids:
      if pipeline not in inspectable_pipelines:
        self.cleanup(pipeline)

  @property
  def tracked_user_pipelines(self):
    return self._tracked_user_pipelines

  def pipeline_id_to_pipeline(self, pid):
    """Converts a pipeline id to a user pipeline.
    """

    pid_to_pipelines = {str(id(p)): p for p in self._tracked_user_pipelines}
    return pid_to_pipelines[pid]

  def mark_pcollection_computed(self, pcolls):
    """Marks computation completeness for the given pcolls.

    Interactive Beam can use this information to determine if a computation is
    needed to introspect the data of any given PCollection.
    """
    self._computed_pcolls.update(pcoll for pcoll in pcolls)

  def evict_computed_pcollections(self, pipeline=None):
    """Evicts all computed PCollections for the given pipeline. If no pipeline
    is specified, evicts for all pipelines.
    """
    if pipeline:
      discarded = set()
      for pcoll in self._computed_pcolls:
        if pcoll.pipeline is pipeline:
          discarded.add(pcoll)
      self._computed_pcolls -= discarded
    else:
      self._computed_pcolls = set()

  @property
  def computed_pcollections(self):
    return self._computed_pcolls

  def load_jquery_with_datatable(self):
    """Loads common resources to enable jquery with datatable configured for
    notebook frontends if necessary. If the resources have been loaded, NOOP.

    A window.interactive_beam_jquery with datatable plugin configured can be
    used in following notebook cells once this is invoked.

    #. There should only be one jQuery imported.
    #. Datatable needs to be imported after jQuery is loaded.
    #. Imported jQuery is attached to window named as jquery[version].
    #. The window attachment needs to happen at the end of import chain until
       all jQuery plugins are set.
    """
    try:
      from IPython.core.display import Javascript
      from IPython.core.display import display_javascript
      display_javascript(
          Javascript(
              _JQUERY_WITH_DATATABLE_TEMPLATE.format(customized_script='')))
    except ImportError:
      pass  # NOOP if dependencies are not available.

  def import_html_to_head(self, html_hrefs):
    """Imports given external HTMLs (supported through webcomponents) into
    the head of the document.

    On load of webcomponentsjs, import given HTMLs. If HTML import is already
    supported, skip loading webcomponentsjs.

    No matter how many times an HTML import occurs in the document, only the
    first occurrence really embeds the external HTML. In a notebook environment,
    the body of the document is always changing due to cell [re-]execution,
    deletion and re-ordering. Thus, HTML imports shouldn't be put in the body
    especially the output areas of notebook cells.
    """
    try:
      from IPython.core.display import Javascript
      from IPython.core.display import display_javascript
      display_javascript(
          Javascript(_HTML_IMPORT_TEMPLATE.format(hrefs=html_hrefs)))
    except ImportError:
      pass  # NOOP if dependencies are not available.
