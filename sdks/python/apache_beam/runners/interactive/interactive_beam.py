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

A convention to import this module:
  from apache_beam.runners.interactive import interactive_beam as ib

Note: If you want backward-compatibility, only invoke interfaces provided by
this module in your notebook or application code.
"""

# pytype: skip-file

import logging
from datetime import timedelta
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import pandas as pd

import apache_beam as beam
from apache_beam.dataframe.frame_base import DeferredBase
from apache_beam.options.pipeline_options import FlinkRunnerOptions
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import DataprocClusterManager
from apache_beam.runners.interactive.dataproc.types import ClusterIdentifier
from apache_beam.runners.interactive.dataproc.types import ClusterMetadata
from apache_beam.runners.interactive.display import pipeline_graph
from apache_beam.runners.interactive.display.pcoll_visualization import visualize
from apache_beam.runners.interactive.display.pcoll_visualization import visualize_computed_pcoll
from apache_beam.runners.interactive.options import interactive_options
from apache_beam.runners.interactive.utils import deferred_df_to_pcollection
from apache_beam.runners.interactive.utils import elements_to_df
from apache_beam.runners.interactive.utils import find_pcoll_name
from apache_beam.runners.interactive.utils import progress_indicated
from apache_beam.runners.runner import PipelineState

_LOGGER = logging.getLogger(__name__)


class Options(interactive_options.InteractiveOptions):
  """Options that guide how Interactive Beam works."""
  @property
  def enable_recording_replay(self):
    """Whether replayable source data recorded should be replayed for multiple
    PCollection evaluations and pipeline runs as long as the data recorded is
    still valid."""
    return self.capture_control._enable_capture_replay

  @enable_recording_replay.setter
  def enable_recording_replay(self, value):
    """Sets whether source data recorded should be replayed. True - Enables
    recording of replayable source data so that following PCollection
    evaluations and pipeline runs always use the same data recorded;
    False - Disables recording of replayable source data so that following
    PCollection evaluation and pipeline runs always use new data from sources.
    """
    # This makes sure the log handler is configured correctly in case the
    # options are configured in an early stage.
    _ = ie.current_env()
    if value:
      _LOGGER.info(
          'Record replay is enabled. When a PCollection is evaluated or the '
          'pipeline is executed, existing data recorded from previous '
          'computations will be replayed for consistent results. If no '
          'recorded data is available, new data from recordable sources will '
          'be recorded.')
    else:
      _LOGGER.info(
          'Record replay is disabled. The next time a PCollection is '
          'evaluated or the pipeline is executed, new data will always be '
          'consumed from sources in the pipeline. You will not have '
          'replayability until re-enabling this option.')
    self.capture_control._enable_capture_replay = value

  @property
  def recordable_sources(self):
    """Interactive Beam automatically records data from sources in this set.
    """
    return self.capture_control._capturable_sources

  @property
  def recording_duration(self):
    """The data recording of sources ends as soon as the background source
    recording job has run for this long."""
    return self.capture_control._capture_duration

  @recording_duration.setter
  def recording_duration(self, value):
    """Sets the recording duration as a timedelta. The input can be a
    datetime.timedelta, a possitive integer as seconds or a string
    representation that is parsable by pandas.to_timedelta.

    Example::

      # Sets the recording duration limit to 10 seconds.
      ib.options.recording_duration = timedelta(seconds=10)
      ib.options.recording_duration = 10
      ib.options.recording_duration = '10s'
      # Explicitly control the recordings.
      ib.recordings.stop(p)
      ib.recordings.clear(p)
      ib.recordings.record(p)
      # The next PCollection evaluation uses fresh data from sources,
      # and the data recorded will be replayed until another clear.
      ib.collect(some_pcoll)
    """
    duration = None
    if isinstance(value, int):
      assert value > 0, 'Duration must be a positive value.'
      duration = timedelta(seconds=value)
    elif isinstance(value, str):
      duration = pd.to_timedelta(value)
    else:
      assert isinstance(value, timedelta), ('The input can only abe a '
        'datetime.timedelta, a possitive integer as seconds, or a string '
        'representation that is parsable by pandas.to_timedelta.')
      duration = value
    if self.capture_control._capture_duration.total_seconds(
    ) != duration.total_seconds():
      _ = ie.current_env()
      _LOGGER.info(
          'You have changed recording duration from %s seconds to %s seconds. '
          'To allow new data to be recorded for the updated duration the '
          'next time a PCollection is evaluated or the pipeline is executed, '
          'please invoke ib.recordings.stop, ib.recordings.clear and '
          'ib.recordings.record.',
          self.capture_control._capture_duration.total_seconds(),
          duration.total_seconds())
      self.capture_control._capture_duration = duration

  @property
  def recording_size_limit(self):
    """The data recording of sources ends as soon as the size (in bytes) of data
    recorded from recordable sources reaches the limit."""
    return self.capture_control._capture_size_limit

  @recording_size_limit.setter
  def recording_size_limit(self, value):
    """Sets the recording size in bytes.

    Example::

      # Sets the recording size limit to 1GB.
      interactive_beam.options.recording_size_limit = 1e9
    """
    if self.capture_control._capture_size_limit != value:
      _ = ie.current_env()
      _LOGGER.info(
          'You have changed recording size limit from %s bytes to %s bytes. To '
          'allow new data to be recorded under the updated size limit the '
          'next time a PCollection is recorded or the pipeline is executed, '
          'please invoke ib.recordings.stop, ib.recordings.clear and '
          'ib.recordings.record.',
          self.capture_control._capture_size_limit,
          value)
      self.capture_control._capture_size_limit = value

  @property
  def display_timestamp_format(self):
    """The format in which timestamps are displayed.

    Default is '%Y-%m-%d %H:%M:%S.%f%z', e.g. 2020-02-01 15:05:06.000015-08:00.
    """
    return self._display_timestamp_format

  @display_timestamp_format.setter
  def display_timestamp_format(self, value):
    """Sets the format in which timestamps are displayed.

    Default is '%Y-%m-%d %H:%M:%S.%f%z', e.g. 2020-02-01 15:05:06.000015-08:00.

    Example::

      # Sets the format to not display the timezone or microseconds.
      interactive_beam.options.display_timestamp_format = %Y-%m-%d %H:%M:%S'
    """
    self._display_timestamp_format = value

  @property
  def display_timezone(self):
    """The timezone in which timestamps are displayed.

    Defaults to local timezone.
    """
    return self._display_timezone

  @display_timezone.setter
  def display_timezone(self, value):
    """Sets the timezone (datetime.tzinfo) in which timestamps are displayed.

    Defaults to local timezone.

    Example::

      # Imports the timezone library.
      from pytz import timezone

      # Will display all timestamps in the US/Eastern time zone.
      tz = timezone('US/Eastern')

      # You can also use dateutil.tz to get a timezone.
      tz = dateutil.tz.gettz('US/Eastern')

      interactive_beam.options.display_timezone = tz
    """
    self._display_timezone = value

  @property
  def cache_root(self):
    """The cache directory specified by the user.

    Defaults to None.
    """
    return self._cache_root

  @cache_root.setter
  def cache_root(self, value):
    """Sets the cache directory.

    Defaults to None.

    Example of local directory usage::
      interactive_beam.options.cache_root = '/Users/username/my/cache/dir'

    Example of GCS directory usage::
      interactive_beam.options.cache_root = 'gs://my-gcs-bucket/cache/dir'
    """
    _LOGGER.warning(
        'Interactive Beam has detected a set value for the cache_root '
        'option. Please note: existing cache managers will not have '
        'their current cache directory changed. The option must be '
        'set in Interactive Beam prior to the initialization of new '
        'pipelines to take effect. To apply changes to new pipelines, '
        'the kernel must be restarted or the pipeline creation codes '
        'must be re-executed. ')
    self._cache_root = value


class Recordings():
  """An introspection interface for recordings for pipelines.

  When a user materializes a PCollection onto disk (eg. ib.show) for a streaming
  pipeline, a background source recording job is started. This job pulls data
  from all defined unbounded sources for that PCollection's pipeline. The
  following methods allow for introspection into that background recording job.
  """
  def describe(self, pipeline=None):
    # type: (Optional[beam.Pipeline]) -> dict[str, Any] # noqa: F821

    """Returns a description of all the recordings for the given pipeline.

    If no pipeline is given then this returns a dictionary of descriptions for
    all pipelines.
    """

    # Create the RecordingManager if it doesn't already exist.
    if pipeline:
      ie.current_env().get_recording_manager(pipeline, create_if_absent=True)

    description = ie.current_env().describe_all_recordings()

    if pipeline:
      return description[pipeline]
    return description

  def clear(self, pipeline):
    # type: (beam.Pipeline) -> bool

    """Clears all recordings of the given pipeline. Returns True if cleared."""

    description = self.describe(pipeline)
    if (not PipelineState.is_terminal(description['state']) and
        description['state'] != PipelineState.STOPPED):
      _LOGGER.warning(
          'Trying to clear a recording with a running pipeline. Did '
          'you forget to call ib.recordings.stop?')
      return False

    ie.current_env().cleanup(pipeline)
    return True

  def stop(self, pipeline):
    # type: (beam.Pipeline) -> None

    """Stops the background source recording of the given pipeline."""

    recording_manager = ie.current_env().get_recording_manager(
        pipeline, create_if_absent=True)
    recording_manager.cancel()

  def record(self, pipeline):
    # type: (beam.Pipeline) -> bool

    """Starts a background source recording job for the given pipeline. Returns
    True if the recording job was started.
    """

    description = self.describe(pipeline)
    if (not PipelineState.is_terminal(description['state']) and
        description['state'] != PipelineState.STOPPED):
      _LOGGER.warning(
          'Trying to start a recording with a running pipeline. Did '
          'you forget to call ib.recordings.stop?')
      return False

    if description['size'] > 0:
      _LOGGER.warning(
          'A recording already exists for this pipeline. To start a '
          'recording, make sure to call ib.recordings.clear first.')
      return False

    recording_manager = ie.current_env().get_recording_manager(
        pipeline, create_if_absent=True)
    return recording_manager.record_pipeline()


class Clusters:
  """An interface to control clusters implicitly created and managed by
  the current interactive environment. This class is not needed and
  should not be used otherwise.

  Do not use it for clusters a user explicitly manages: e.g., if you have
  a Flink cluster running somewhere and provides the flink master when
  running a pipeline with the FlinkRunner, the cluster will not be tracked
  or managed by Beam.
  To reuse the same cluster for your pipelines, use the same pipeline
  options: e.g., a pipeline option with the same flink master if you are
  using FlinkRunner.

  This module is experimental. No backwards-compatibility guarantees.

  Interactive Beam automatically creates/reuses existing worker clusters to
  execute pipelines when it detects the need from configurations.
  Currently, the only supported cluster implementation is Flink running on
  Cloud Dataproc.

  To configure a pipeline to run on Cloud Dataproc with Flink, set the
  underlying runner of the InteractiveRunner to FlinkRunner and the pipeline
  options to indicate where on Cloud the FlinkRunner should be deployed to.

    An example to enable automatic Dataproc cluster creation/reuse::

      options = PipelineOptions([
          '--project=my-project',
          '--region=my-region',
          '--environment_type=DOCKER'])
      pipeline = beam.Pipeline(InteractiveRunner(
          underlying_runner=FlinkRunner()), options=options)

  Reuse a pipeline options in another pipeline would configure Interactive Beam
  to reuse the same Dataproc cluster implicitly managed by the current
  interactive environment.
  If a flink_master is identified as a known cluster, the corresponding cluster
  is also resued.
  Furthermore, if a cluster is explicitly created by using a pipeline as an
  identifier to a known cluster, the cluster is reused.

    An example::

      # If pipeline runs on a known cluster, below code reuses the cluster
      # manager without creating a new one.
      dcm = ib.clusters.create(pipeline)

  To provision the cluster, use WorkerOptions. Supported configurations are::

    1. subnetwork
    2. num_workers
    3. machine_type

  To configure a pipeline to run on an existing FlinkRunner deployed elsewhere,
  set the flink_master explicitly so no cluster will be created/reused.

    An example pipeline options to skip automatic Dataproc cluster usage::

      options = PipelineOptions([
          '--flink_master=some.self.hosted.flink:port',
          '--environment_type=DOCKER'])

  To configure a pipeline to run on a local FlinkRunner, explicitly set the
  default cluster metadata to None: ib.clusters.set_default_cluster(None).
  """
  # Explicitly set the Flink version here to ensure compatibility with 2.1
  # Dataproc images:
  # https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.1
  DATAPROC_FLINK_VERSION = '1.15'

  # The minimum worker number to create a Dataproc cluster.
  DATAPROC_MINIMUM_WORKER_NUM = 2

  # TODO(https://github.com/apache/beam/issues/21527): Fix the Dataproc image
  # version after a released image contains all missing dependencies for Flink
  # to run.
  # DATAPROC_IMAGE_VERSION = '2.0.XX-debian10'

  def __init__(self) -> None:
    self.dataproc_cluster_managers: Dict[ClusterMetadata,
                                         DataprocClusterManager] = {}
    self.master_urls: Dict[str, ClusterMetadata] = {}
    self.pipelines: Dict[beam.Pipeline, DataprocClusterManager] = {}
    self.default_cluster_metadata: Optional[ClusterMetadata] = None

  def create(
      self, cluster_identifier: ClusterIdentifier) -> DataprocClusterManager:
    """Creates a Dataproc cluster manager provisioned for the cluster
    identified. If the cluster is known, returns an existing cluster manager.
    """
    # Try to get some not-None cluster metadata.
    cluster_metadata = self.cluster_metadata(cluster_identifier)
    if not cluster_metadata:
      raise ValueError(
          'Unknown cluster identifier: %s. Cannot create or reuse'
          'a Dataproc cluster.')
    if not cluster_metadata.region:
      _LOGGER.info(
          'No region information was detected, defaulting Dataproc cluster '
          'region to: us-central1.')
      cluster_metadata.region = 'us-central1'
    elif cluster_metadata.region == 'global':
      # The global region is unsupported as it will be eventually deprecated.
      raise ValueError('Clusters in the global region are not supported.')
    # else use the provided region.
    if (cluster_metadata.num_workers and
        cluster_metadata.num_workers < self.DATAPROC_MINIMUM_WORKER_NUM):
      _LOGGER.info(
          'At least %s workers are required for a cluster, defaulting to %s.',
          self.DATAPROC_MINIMUM_WORKER_NUM,
          self.DATAPROC_MINIMUM_WORKER_NUM)
      cluster_metadata.num_workers = self.DATAPROC_MINIMUM_WORKER_NUM
    known_dcm = self.dataproc_cluster_managers.get(cluster_metadata, None)
    if known_dcm:
      return known_dcm
    dcm = DataprocClusterManager(cluster_metadata)
    dcm.create_flink_cluster()
    # ClusterMetadata with derivative fields populated by the dcm.
    derived_meta = dcm.cluster_metadata
    self.dataproc_cluster_managers[derived_meta] = dcm
    self.master_urls[derived_meta.master_url] = derived_meta
    # Update the default cluster metadata to the one just created.
    self.set_default_cluster(derived_meta)
    return dcm

  def cleanup(
      self,
      cluster_identifier: Optional[ClusterIdentifier] = None,
      force: bool = False) -> None:
    """Cleans up the cluster associated with the given cluster_identifier.

    When None cluster_identifier is provided: if force is True, cleans up for
    all clusters; otherwise, do a dry run and NOOP.
    If a beam.Pipeline is given as the ClusterIdentifier while multiple
    pipelines share the same cluster, it only cleans up the association between
    the pipeline and the cluster identified.
    If the cluster_identifier is unknown, NOOP.
    """
    if not cluster_identifier:
      dcm_to_cleanup = set(self.dataproc_cluster_managers.values())
      if force:
        for dcm in dcm_to_cleanup:
          self._cleanup(dcm)
        self.default_cluster_metadata = None
      else:
        _LOGGER.warning(
            'No cluster_identifier provided. If you intend to '
            'clean up all clusters, invoke ib.clusters.cleanup(force=True). '
            'Current clusters are %s.',
            self.describe())
    elif isinstance(cluster_identifier, beam.Pipeline):
      p = cluster_identifier
      dcm = self.pipelines.pop(p, None)
      if dcm:
        dcm.pipelines.remove(p)
        p_flink_options = p.options.view_as(FlinkRunnerOptions)
        p_flink_options.flink_master = '[auto]'
        p_flink_options.flink_version = None
        # Only cleans up when there is no pipeline using the cluster.
        if not dcm.pipelines:
          self._cleanup(dcm)
    else:
      if isinstance(cluster_identifier, str):
        meta = self.master_urls.get(cluster_identifier, None)
      else:
        meta = cluster_identifier
      dcm = self.dataproc_cluster_managers.get(meta, None)
      if dcm:
        self._cleanup(dcm)

  def describe(
      self,
      cluster_identifier: Optional[ClusterIdentifier] = None
  ) -> Union[ClusterMetadata, List[ClusterMetadata]]:
    """Describes the ClusterMetadata by a ClusterIdentifier.

    If no cluster_identifier is given or if the cluster_identifier is unknown,
    it returns descriptions for all known clusters.

    Example usage:
    # Describe the cluster executing work for a pipeline.
    ib.clusters.describe(pipeline)
    # Describe the cluster with the flink master url.
    ib.clusters.describe(master_url)
    # Describe all existing clusters.
    ib.clusters.describe()
    """
    if cluster_identifier:
      meta = self._cluster_metadata(cluster_identifier)
      if meta in self.dataproc_cluster_managers:
        return meta
    return list(self.dataproc_cluster_managers.keys())

  def set_default_cluster(
      self, cluster_identifier: Optional[ClusterIdentifier] = None) -> None:
    """Temporarily sets the default metadata for creating or reusing a
    DataprocClusterManager. It is always updated to the most recently created
    cluster.

    If no known ClusterMetadata can be identified by the ClusterIdentifer, NOOP.
    If None is set, next time when Flink is in use, if no cluster is explicitly
    configured by a pipeline, the job runs locally.
    """
    if cluster_identifier:
      self.default_cluster_metadata = self.cluster_metadata(cluster_identifier)
    else:
      self.default_cluster_metadata = None

  def cluster_metadata(
      self,
      cluster_identifier: Optional[ClusterIdentifier] = None
  ) -> Optional[ClusterMetadata]:
    """Fetches the ClusterMetadata by a ClusterIdentifier that could be a
    URL in string, a Beam pipeline, or an equivalent to a known ClusterMetadata;

    If the given cluster_identifier is an URL or a pipeline that is unknown to
    the current environment, the default cluster metadata (could be None) is
    returned.
    If the given cluster_identifier is a ClusterMetadata but unknown to the
    current environment, passes it through (NOOP).
    """
    meta = self._cluster_metadata(cluster_identifier)
    return meta if meta else self.default_cluster_metadata

  def _cluster_metadata(
      self,
      cluster_identifier: Optional[ClusterIdentifier] = None
  ) -> Optional[ClusterMetadata]:
    meta = None
    if cluster_identifier:
      if isinstance(cluster_identifier, str):
        meta = self.master_urls.get(cluster_identifier, None)
      elif isinstance(cluster_identifier, beam.Pipeline):
        dcm = self.pipelines.get(cluster_identifier, None)
        if dcm:
          meta = dcm.cluster_metadata
      elif isinstance(cluster_identifier, ClusterMetadata):
        meta = cluster_identifier
        if meta in self.dataproc_cluster_managers:
          meta = self.dataproc_cluster_managers[meta].cluster_metadata
        elif (meta and self.default_cluster_metadata and
              meta.cluster_name == self.default_cluster_metadata.cluster_name):
          _LOGGER.warning(
              'Cannot change the configuration of the running cluster %s. '
              'Existing is %s, desired is %s.',
              self.default_cluster_metadata.cluster_name,
              self.default_cluster_metadata,
              meta)
          meta.reset_name()
          _LOGGER.warning(
              'To avoid conflict, issuing a new cluster name %s '
              'for a new cluster.',
              meta.cluster_name)
      else:
        raise TypeError(
            'A cluster_identifier should be Optional[Union[str, '
            'beam.Pipeline, ClusterMetadata], instead %s was given.',
            type(cluster_identifier))
    return meta

  def _cleanup(self, dcm: DataprocClusterManager) -> None:
    dcm.cleanup()
    self.dataproc_cluster_managers.pop(dcm.cluster_metadata, None)
    self.master_urls.pop(dcm.cluster_metadata.master_url, None)
    for p in dcm.pipelines:
      self.pipelines.pop(p, None)
    if dcm.cluster_metadata == self.default_cluster_metadata:
      self.default_cluster_metadata = None


# Users can set options to guide how Interactive Beam works.
# Examples:
# ib.options.enable_recording_replay = False/True
# ib.options.recording_duration = '1m'
# ib.options.recordable_sources.add(SourceClass)
# Check the docstrings for detailed usages.
options = Options()

# Users can introspect into recordings by using the recordings class.
# Examples:
# p = beam.Pipeline(InteractiveRunner())
# elems = p | beam.Create([1, 2, 3])
# ib.show(elems)
# ib.recordings.describe(p)
recordings = Recordings()

# Users can interact with the clusters used by their environment.
# Examples:
# ib.clusters.describe(p)
# Check the docstrings for detailed usages.
clusters = Clusters()


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

      show(init_pcoll)

    To visualize data from init_pcoll once the pipeline is executed.
  """
  ie.current_env().watch(watchable)


@progress_indicated
def show(
    *pcolls,
    include_window_info=False,
    visualize_data=False,
    n='inf',
    duration='inf'):
  # type: (*Union[Dict[Any, PCollection], Iterable[PCollection], PCollection], bool, bool, Union[int, str], Union[int, str]) -> None # noqa: F821

  """Shows given PCollections in an interactive exploratory way if used within
  a notebook, or prints a heading sampled data if used within an ipython shell.
  Noop if used in a non-interactive environment.

  Args:
    include_window_info: (optional) if True, windowing information of the
        data will be visualized too. Default is false.
    visualize_data: (optional) by default, the visualization contains data
        tables rendering data from given pcolls separately as if they are
        converted into dataframes. If visualize_data is True, there will be a
        more dive-in widget and statistically overview widget of the data.
        Otherwise, those 2 data visualization widgets will not be displayed.
    n: (optional) max number of elements to visualize. Default 'inf'.
    duration: (optional) max duration of elements to read in integer seconds or
        a string duration. Default 'inf'.

  The given pcolls can be dictionary of PCollections (as values), or iterable
  of PCollections or plain PCollection values.

  The user can specify either the max number of elements with `n` to read
  or the maximum duration of elements to read with `duration`. When a limiter is
  not supplied, it is assumed to be infinite.

  By default, the visualization contains data tables rendering data from given
  pcolls separately as if they are converted into dataframes. If visualize_data
  is True, there will be a more dive-in widget and statistically overview widget
  of the data. Otherwise, those 2 data visualization widgets will not be
  displayed.

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
  flatten_pcolls = []
  for pcoll_container in pcolls:
    if isinstance(pcoll_container, dict):
      flatten_pcolls.extend(pcoll_container.values())
    elif isinstance(pcoll_container, (beam.pvalue.PCollection, DeferredBase)):
      flatten_pcolls.append(pcoll_container)
    else:
      try:
        flatten_pcolls.extend(iter(pcoll_container))
      except TypeError:
        raise ValueError(
            'The given pcoll %s is not a dict, an iterable or a PCollection.' %
            pcoll_container)

  # Iterate through the given PCollections and convert any deferred DataFrames
  # or Series into PCollections.
  pcolls = set()

  # The element type is used to help visualize the given PCollection. For the
  # deferred DataFrame/Series case it is the proxy of the frame.
  element_types = {}
  for pcoll in flatten_pcolls:
    if isinstance(pcoll, DeferredBase):
      pcoll, element_type = deferred_df_to_pcollection(pcoll)
      watch({'anonymous_pcollection_{}'.format(id(pcoll)): pcoll})
    else:
      element_type = pcoll.element_type

    element_types[pcoll] = element_type

    pcolls.add(pcoll)
    assert isinstance(pcoll, beam.pvalue.PCollection), (
        '{} is not an apache_beam.pvalue.PCollection.'.format(pcoll))

  assert len(pcolls) > 0, (
      'Need at least 1 PCollection to show data visualization.')

  pcoll_pipeline = next(iter(pcolls)).pipeline
  user_pipeline = ie.current_env().user_pipeline(pcoll_pipeline)
  # Possibly showing a PCollection defined in a local scope that is not
  # explicitly watched. Ad hoc watch it though it's a little late.
  if not user_pipeline:
    watch({'anonymous_pipeline_{}'.format(id(pcoll_pipeline)): pcoll_pipeline})
    user_pipeline = pcoll_pipeline

  if isinstance(n, str):
    assert n == 'inf', (
        'Currently only the string \'inf\' is supported. This denotes reading '
        'elements until the recording is stopped via a kernel interrupt.')
  elif isinstance(n, int):
    assert n > 0, 'n needs to be positive or the string \'inf\''

  if isinstance(duration, int):
    assert duration > 0, ('duration needs to be positive, a duration string, '
                          'or the string \'inf\'')

  if n == 'inf':
    n = float('inf')

  if duration == 'inf':
    duration = float('inf')

  previously_computed_pcolls = {
      pcoll
      for pcoll in pcolls if pcoll in ie.current_env().computed_pcollections
  }
  for pcoll in previously_computed_pcolls:
    visualize_computed_pcoll(
        find_pcoll_name(pcoll),
        pcoll,
        n,
        duration,
        include_window_info=include_window_info,
        display_facets=visualize_data)
  pcolls = pcolls - previously_computed_pcolls

  recording_manager = ie.current_env().get_recording_manager(
      user_pipeline, create_if_absent=True)
  recording = recording_manager.record(pcolls, max_n=n, max_duration=duration)

  # Catch a KeyboardInterrupt to gracefully cancel the recording and
  # visualizations.
  try:
    # If in notebook, static plotting computed pcolls as computation is done.
    if ie.current_env().is_in_notebook:
      for stream in recording.computed().values():
        visualize(
            stream,
            include_window_info=include_window_info,
            display_facets=visualize_data,
            element_type=element_types[stream.pcoll])
    elif ie.current_env().is_in_ipython:
      for stream in recording.computed().values():
        visualize(
            stream,
            include_window_info=include_window_info,
            element_type=element_types[stream.pcoll])
    if recording.is_computed():
      return

    # If in notebook, dynamic plotting as computation goes.
    if ie.current_env().is_in_notebook:
      for stream in recording.uncomputed().values():
        visualize(
            stream,
            dynamic_plotting_interval=1,
            include_window_info=include_window_info,
            display_facets=visualize_data,
            element_type=element_types[stream.pcoll])

    # Invoke wait_until_finish to ensure the blocking nature of this API without
    # relying on the run to be blocking.
    recording.wait_until_finish()

    # If just in ipython shell, plotting once when the computation is completed.
    if ie.current_env().is_in_ipython and not ie.current_env().is_in_notebook:
      for stream in recording.computed().values():
        visualize(stream, include_window_info=include_window_info)

  except KeyboardInterrupt:
    if recording:
      recording.cancel()


@progress_indicated
def collect(
    *pcolls,
    n='inf',
    duration='inf',
    include_window_info=False,
    runner=None,
    options=None,
    force_compute=False,
    force_tuple=False):
  """Materializes the elements from a PCollection into a Dataframe.

  This reads each element from file and reads only the amount that it needs
  into memory. The user can specify either the max number of elements to read
  or the maximum duration of elements to read. When a limiter is not supplied,
  it is assumed to be infinite.

  Args:
    pcolls: PCollections to compute.
    n: (optional) max number of elements to visualize. Default 'inf'.
    duration: (optional) max duration of elements to read in integer seconds or
        a string duration. Default 'inf'.
    include_window_info: (optional) if True, appends the windowing information
        to each row. Default False.
    runner: (optional) the runner with which to compute the results
    options: (optional) any additional pipeline options to use to compute the
        results
    force_compute: (optional) if True, forces recomputation rather than using
        cached PCollections
    force_tuple: (optional) if True, return a 1-tuple or results rather than
        the bare results if only one PCollection is computed

  For example::

    p = beam.Pipeline(InteractiveRunner())
    init = p | 'Init' >> beam.Create(range(10))
    square = init | 'Square' >> beam.Map(lambda x: x * x)

    # Run the pipeline and bring the PCollection into memory as a Dataframe.
    in_memory_square = head(square, n=5)
  """
  if len(pcolls) == 0:
    return ()

  def as_pcollection(pcoll_or_df):
    if isinstance(pcoll_or_df, DeferredBase):
      # Get the proxy so we can get the output shape of the DataFrame.
      pcoll, element_type = deferred_df_to_pcollection(pcoll_or_df)
      watch({'anonymous_pcollection_{}'.format(id(pcoll)): pcoll})
      return pcoll, element_type
    elif isinstance(pcoll_or_df, beam.pvalue.PCollection):
      return pcoll_or_df, pcoll_or_df.element_type
    else:
      raise TypeError(f'{pcoll} is not an apache_beam.pvalue.PCollection.')

  pcolls_with_element_types = [as_pcollection(p) for p in pcolls]
  pcolls_to_element_types = dict(pcolls_with_element_types)
  pcolls = [pcoll for pcoll, _ in pcolls_with_element_types]
  pipelines = set(pcoll.pipeline for pcoll in pcolls)
  if len(pipelines) != 1:
    raise ValueError('All PCollections must belong to the same pipeline.')
  pipeline, = pipelines

  if isinstance(n, str):
    assert n == 'inf', (
        'Currently only the string \'inf\' is supported. This denotes reading '
        'elements until the recording is stopped via a kernel interrupt.')
  elif isinstance(n, int):
    assert n > 0, 'n needs to be positive or the string \'inf\''

  if isinstance(duration, int):
    assert duration > 0, ('duration needs to be positive, a duration string, '
                          'or the string \'inf\'')

  if n == 'inf':
    n = float('inf')

  if duration == 'inf':
    duration = float('inf')

  user_pipeline = ie.current_env().user_pipeline(pipeline)
  # Possibly collecting a PCollection defined in a local scope that is not
  # explicitly watched. Ad hoc watch it though it's a little late.
  if not user_pipeline:
    watch({'anonymous_pipeline_{}'.format(id(pipeline)): pipeline})
    user_pipeline = pipeline
  recording_manager = ie.current_env().get_recording_manager(
      user_pipeline, create_if_absent=True)

  # If already computed, directly read the stream and return.
  computed = {}
  for pcoll in pcolls_to_element_types.keys():
    if pcoll in ie.current_env().computed_pcollections and not force_compute:
      pcoll_name = find_pcoll_name(pcoll)
      computed[pcoll] = list(
          recording_manager.read(pcoll_name, pcoll, n, duration).read())

  uncomputed = set(pcolls) - set(computed.keys())
  if uncomputed:
    recording = recording_manager.record(
        uncomputed,
        max_n=n,
        max_duration=duration,
        runner=runner,
        options=options,
        force_compute=force_compute)

    try:
      for pcoll in uncomputed:
        computed[pcoll] = list(recording.stream(pcoll).read())
    except KeyboardInterrupt:
      recording.cancel()

  if n == float('inf'):
    n = None

  # Collecting DataFrames may have a length > n, so slice again to be sure. Note
  # that array[:None] returns everything.
  empty = pd.DataFrame()
  result_tuple = tuple(
      elements_to_df(
          computed[pcoll],
          include_window_info=include_window_info,
          element_type=pcolls_to_element_types[pcoll])[:n] if pcoll in
      computed else empty for pcoll in pcolls)

  if len(result_tuple) == 1 and not force_tuple:
    return result_tuple[0]
  else:
    return result_tuple


@progress_indicated
def show_graph(pipeline):
  """Shows the current pipeline shape of a given Beam pipeline as a DAG.
  """
  pipeline_graph.PipelineGraph(pipeline).display_graph()


def evict_recorded_data(pipeline=None):
  """Forcefully evicts all recorded replayable data for the given pipeline. If
  no pipeline is specified, evicts for all user defined pipelines.

  Once invoked, Interactive Beam will record new data based on the guidance of
  options the next time it evaluates/visualizes PCollections or runs pipelines.
  """
  from apache_beam.runners.interactive.options import capture_control
  capture_control.evict_captured_data(pipeline)
