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

"""Pipeline options obtained from command line parsing."""

from __future__ import absolute_import

import argparse
import logging
from builtins import list
from builtins import object

from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.transforms.display import HasDisplayData

__all__ = [
    'PipelineOptions',
    'StandardOptions',
    'TypeOptions',
    'DirectOptions',
    'GoogleCloudOptions',
    'HadoopFileSystemOptions',
    'WorkerOptions',
    'DebugOptions',
    'ProfilingOptions',
    'SetupOptions',
    'TestOptions',
    ]


def _static_value_provider_of(value_type):
  """"Helper function to plug a ValueProvider into argparse.

  Args:
    value_type: the type of the value. Since the type param of argparse's
                add_argument will always be ValueProvider, we need to
                preserve the type of the actual value.
  Returns:
    A partially constructed StaticValueProvider in the form of a function.

  """
  def _f(value):
    _f.__name__ = value_type.__name__
    return StaticValueProvider(value_type, value)
  return _f


class _BeamArgumentParser(argparse.ArgumentParser):
  """An ArgumentParser that supports ValueProvider options.

  Example Usage::

    class TemplateUserOptions(PipelineOptions):
      @classmethod

      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--vp-arg1', default='start')
        parser.add_value_provider_argument('--vp-arg2')
        parser.add_argument('--non-vp-arg')

  """
  def add_value_provider_argument(self, *args, **kwargs):
    """ValueProvider arguments can be either of type keyword or positional.
    At runtime, even positional arguments will need to be supplied in the
    key/value form.
    """
    # Extract the option name from positional argument ['pos_arg']
    assert args != () and len(args[0]) >= 1
    if args[0][0] != '-':
      option_name = args[0]
      if kwargs.get('nargs') is None:  # make them optionally templated
        kwargs['nargs'] = '?'
    else:
      # or keyword arguments like [--kw_arg, -k, -w] or [--kw-arg]
      option_name = [i.replace('--', '') for i in args if i[:2] == '--'][0]

    # reassign the type to make room for using
    # StaticValueProvider as the type for add_argument
    value_type = kwargs.get('type') or str
    kwargs['type'] = _static_value_provider_of(value_type)

    # reassign default to default_value to make room for using
    # RuntimeValueProvider as the default for add_argument
    default_value = kwargs.get('default')
    kwargs['default'] = RuntimeValueProvider(
        option_name=option_name,
        value_type=value_type,
        default_value=default_value
    )

    # have add_argument do most of the work
    self.add_argument(*args, **kwargs)

  # The argparse package by default tries to autocomplete option names. This
  # results in an "ambiguous option" error from argparse when an unknown option
  # matching multiple known ones are used. This suppresses that behavior.
  def error(self, message):
    if message.startswith('ambiguous option: '):
      return
    super(_BeamArgumentParser, self).error(message)


class PipelineOptions(HasDisplayData):
  """Pipeline options class used as container for command line options.

  The class is essentially a wrapper over the standard argparse Python module
  (see https://docs.python.org/3/library/argparse.html).  To define one option
  or a group of options you subclass from PipelineOptions::

    class XyzOptions(PipelineOptions):

      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument('--abc', default='start')
        parser.add_argument('--xyz', default='end')

  The arguments for the add_argument() method are exactly the ones
  described in the argparse public documentation.

  Pipeline objects require an options object during initialization.
  This is obtained simply by initializing an options class as defined above::

    p = Pipeline(options=XyzOptions())
    if p.options.xyz == 'end':
      raise ValueError('Option xyz has an invalid value.')

  By default the options classes will use command line arguments to initialize
  the options.
  """
  def __init__(self, flags=None, **kwargs):
    """Initialize an options class.

    The initializer will traverse all subclasses, add all their argparse
    arguments and then parse the command line specified by flags or by default
    the one obtained from sys.argv.

    The subclasses are not expected to require a redefinition of __init__.

    Args:
      flags: An iterable of command line arguments to be used. If not specified
        then sys.argv will be used as input for parsing arguments.

      **kwargs: Add overrides for arguments passed in flags.
    """
    self._flags = flags
    self._all_options = kwargs
    parser = _BeamArgumentParser()

    for cls in type(self).mro():
      if cls == PipelineOptions:
        break
      elif '_add_argparse_args' in cls.__dict__:
        cls._add_argparse_args(parser)
    # The _visible_options attribute will contain only those options from the
    # flags (i.e., command line) that can be recognized. The _all_options
    # field contains additional overrides.
    self._visible_options, _ = parser.parse_known_args(flags)

  @classmethod
  def _add_argparse_args(cls, parser):
    # Override this in subclasses to provide options.
    pass

  @classmethod
  def from_dictionary(cls, options):
    """Returns a PipelineOptions from a dictionary of arguments.

    Args:
      options: Dictionary of argument value pairs.

    Returns:
      A PipelineOptions object representing the given arguments.
    """
    flags = []
    for k, v in options.items():
      if isinstance(v, bool):
        if v:
          flags.append('--%s' % k)
      elif isinstance(v, list):
        for i in v:
          flags.append('--%s=%s' % (k, i))
      else:
        flags.append('--%s=%s' % (k, v))

    return cls(flags)

  def get_all_options(self, drop_default=False, add_extra_args_fn=None):
    """Returns a dictionary of all defined arguments.

    Returns a dictionary of all defined arguments (arguments that are defined in
    any subclass of PipelineOptions) into a dictionary.

    Args:
      drop_default: If set to true, options that are equal to their default
        values, are not returned as part of the result dictionary.
      add_extra_args_fn: Callback to populate additional arguments, can be used
        by runner to supply otherwise unknown args.

    Returns:
      Dictionary of all args and values.
    """

    # TODO(BEAM-1319): PipelineOption sub-classes in the main session might be
    # repeated. Pick last unique instance of each subclass to avoid conflicts.
    subset = {}
    parser = _BeamArgumentParser()
    for cls in PipelineOptions.__subclasses__():
      subset[str(cls)] = cls
    for cls in subset.values():
      cls._add_argparse_args(parser)  # pylint: disable=protected-access
    if add_extra_args_fn:
      add_extra_args_fn(parser)
    known_args, unknown_args = parser.parse_known_args(self._flags)
    if unknown_args:
      logging.warning("Discarding unparseable args: %s", unknown_args)
    result = vars(known_args)

    # Apply the overrides if any
    for k in list(result):
      if k in self._all_options:
        result[k] = self._all_options[k]
      if (drop_default and
          parser.get_default(k) == result[k] and
          not isinstance(parser.get_default(k), ValueProvider)):
        del result[k]

    return result

  def display_data(self):
    return self.get_all_options(True)

  def view_as(self, cls):
    view = cls(self._flags)
    view._all_options = self._all_options
    return view

  def _visible_option_list(self):
    return sorted(option
                  for option in dir(self._visible_options) if option[0] != '_')

  def __dir__(self):
    return sorted(dir(type(self)) + list(self.__dict__) +
                  self._visible_option_list())

  def __getattr__(self, name):
    # Special methods which may be accessed before the object is
    # fully constructed (e.g. in unpickling).
    if name[:2] == name[-2:] == '__':
      return object.__getattribute__(self, name)
    elif name in self._visible_option_list():
      return self._all_options.get(name, getattr(self._visible_options, name))
    else:
      raise AttributeError("'%s' object has no attribute '%s'" %
                           (type(self).__name__, name))

  def __setattr__(self, name, value):
    if name in ('_flags', '_all_options', '_visible_options'):
      super(PipelineOptions, self).__setattr__(name, value)
    elif name in self._visible_option_list():
      self._all_options[name] = value
    else:
      raise AttributeError("'%s' object has no attribute '%s'" %
                           (type(self).__name__, name))

  def __str__(self):
    return '%s(%s)' % (type(self).__name__,
                       ', '.join('%s=%s' % (option, getattr(self, option))
                                 for option in self._visible_option_list()))


class StandardOptions(PipelineOptions):

  DEFAULT_RUNNER = 'DirectRunner'

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--runner',
        help=('Pipeline runner used to execute the workflow. Valid values are '
              'DirectRunner, DataflowRunner.'))
    # Whether to enable streaming mode.
    parser.add_argument('--streaming',
                        default=False,
                        action='store_true',
                        help='Whether to enable streaming mode.')


class TypeOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    # TODO(laolu): Add a type inferencing option here once implemented.
    parser.add_argument('--type_check_strictness',
                        default='DEFAULT_TO_ANY',
                        choices=['ALL_REQUIRED', 'DEFAULT_TO_ANY'],
                        help='The level of exhaustive manual type-hint '
                        'annotation required')
    parser.add_argument('--no_pipeline_type_check',
                        dest='pipeline_type_check',
                        action='store_false',
                        help='Disable type checking at pipeline construction '
                        'time')
    parser.add_argument('--runtime_type_check',
                        default=False,
                        action='store_true',
                        help='Enable type checking at pipeline execution '
                        'time. NOTE: only supported with the '
                        'DirectRunner')


class DirectOptions(PipelineOptions):
  """DirectRunner-specific execution options."""

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--no_direct_runner_use_stacked_bundle',
        action='store_false',
        dest='direct_runner_use_stacked_bundle',
        help='DirectRunner uses stacked WindowedValues within a Bundle for '
        'memory optimization. Set --no_direct_runner_use_stacked_bundle to '
        'avoid it.')
    parser.add_argument(
        '--direct_runner_bundle_repeat',
        type=int,
        default=0,
        help='replay every bundle this many extra times, for profiling'
        'and debugging')


class GoogleCloudOptions(PipelineOptions):
  """Google Cloud Dataflow service execution options."""

  BIGQUERY_API_SERVICE = 'bigquery.googleapis.com'
  COMPUTE_API_SERVICE = 'compute.googleapis.com'
  STORAGE_API_SERVICE = 'storage.googleapis.com'
  DATAFLOW_ENDPOINT = 'https://dataflow.googleapis.com'

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--dataflow_endpoint',
        default=cls.DATAFLOW_ENDPOINT,
        help=
        ('The URL for the Dataflow API. If not set, the default public URL '
         'will be used.'))
    # Remote execution must check that this option is not None.
    parser.add_argument('--project',
                        default=None,
                        help='Name of the Cloud project owning the Dataflow '
                        'job.')
    # Remote execution must check that this option is not None.
    parser.add_argument('--job_name',
                        default=None,
                        help='Name of the Cloud Dataflow job.')
    # Remote execution must check that this option is not None.
    parser.add_argument('--staging_location',
                        default=None,
                        help='GCS path for staging code packages needed by '
                        'workers.')
    # Remote execution must check that this option is not None.
    # If staging_location is not set, it defaults to temp_location.
    parser.add_argument('--temp_location',
                        default=None,
                        help='GCS path for saving temporary workflow jobs.')
    # The Cloud Dataflow service does not yet honor this setting. However, once
    # service support is added then users of this SDK will be able to control
    # the region. Default is up to the Dataflow service. See
    # https://cloud.google.com/compute/docs/regions-zones/regions-zones for a
    # list of valid options/
    parser.add_argument('--region',
                        default='us-central1',
                        help='The Google Compute Engine region for creating '
                        'Dataflow job.')
    parser.add_argument('--service_account_email',
                        default=None,
                        help='Identity to run virtual machines as.')
    parser.add_argument('--no_auth', dest='no_auth', type=bool, default=False)
    # Option to run templated pipelines
    parser.add_argument('--template_location',
                        default=None,
                        help='Save job to specified local or GCS location.')
    parser.add_argument('--label', '--labels',
                        dest='labels',
                        action='append',
                        default=None,
                        help='Labels to be applied to this Dataflow job. '
                        'Labels are key value pairs separated by = '
                        '(e.g. --label key=value).')
    parser.add_argument('--update',
                        default=False,
                        action='store_true',
                        help='Update an existing streaming Cloud Dataflow job. '
                        'Experimental. '
                        'See https://cloud.google.com/dataflow/pipelines/'
                        'updating-a-pipeline')
    parser.add_argument('--enable_streaming_engine',
                        default=False,
                        action='store_true',
                        help='Enable Windmill Service for this Dataflow job. ')
    parser.add_argument('--dataflow_kms_key',
                        default=None,
                        help='Set a Google Cloud KMS key name to be used in '
                        'Dataflow state operations (GBK, Streaming).')
    parser.add_argument('--flexrs_goal',
                        default=None,
                        choices=['COST_OPTIMIZED', 'SPEED_OPTIMIZED'],
                        help='Set the Flexible Resource Scheduling mode')

  def validate(self, validator):
    errors = []
    if validator.is_service_runner():
      errors.extend(validator.validate_cloud_options(self))
      errors.extend(validator.validate_gcs_path(self, 'temp_location'))
      if getattr(self, 'staging_location',
                 None) or getattr(self, 'temp_location', None) is None:
        errors.extend(validator.validate_gcs_path(self, 'staging_location'))

    if self.view_as(DebugOptions).dataflow_job_file:
      if self.view_as(GoogleCloudOptions).template_location:
        errors.append('--dataflow_job_file and --template_location '
                      'are mutually exclusive.')

    return errors


class HadoopFileSystemOptions(PipelineOptions):
  """``HadoopFileSystem`` connection options."""

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--hdfs_host',
        default=None,
        help=
        ('Hostname or address of the HDFS namenode.'))
    parser.add_argument(
        '--hdfs_port',
        default=None,
        help=
        ('Port of the HDFS namenode.'))
    parser.add_argument(
        '--hdfs_user',
        default=None,
        help=
        ('HDFS username to use.'))

  def validate(self, validator):
    errors = []
    errors.extend(validator.validate_optional_argument_positive(self, 'port'))
    return errors


# Command line options controlling the worker pool configuration.
# TODO(silviuc): Update description when autoscaling options are in.
class WorkerOptions(PipelineOptions):
  """Worker pool configuration options."""

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--num_workers',
        type=int,
        default=None,
        help=
        ('Number of workers to use when executing the Dataflow job. If not '
         'set, the Dataflow service will use a reasonable default.'))
    parser.add_argument(
        '--max_num_workers',
        type=int,
        default=None,
        help=
        ('Maximum number of workers to use when executing the Dataflow job.'))
    parser.add_argument(
        '--autoscaling_algorithm',
        type=str,
        choices=['NONE', 'THROUGHPUT_BASED'],
        default=None,  # Meaning unset, distinct from 'NONE' meaning don't scale
        help=
        ('If and how to autoscale the workerpool.'))
    parser.add_argument(
        '--worker_machine_type',
        dest='machine_type',
        default=None,
        help=('Machine type to create Dataflow worker VMs as. See '
              'https://cloud.google.com/compute/docs/machine-types '
              'for a list of valid options. If not set, '
              'the Dataflow service will choose a reasonable '
              'default.'))
    parser.add_argument(
        '--disk_size_gb',
        type=int,
        default=None,
        help=
        ('Remote worker disk size, in gigabytes, or 0 to use the default size. '
         'If not set, the Dataflow service will use a reasonable default.'))
    parser.add_argument(
        '--worker_disk_type',
        dest='disk_type',
        default=None,
        help=('Specifies what type of persistent disk should be used.'))
    parser.add_argument(
        '--zone',
        default=None,
        help=(
            'GCE availability zone for launching workers. Default is up to the '
            'Dataflow service.'))
    parser.add_argument(
        '--network',
        default=None,
        help=(
            'GCE network for launching workers. Default is up to the Dataflow '
            'service.'))
    parser.add_argument(
        '--subnetwork',
        default=None,
        help=(
            'GCE subnetwork for launching workers. Default is up to the '
            'Dataflow service. Expected format is '
            'regions/REGION/subnetworks/SUBNETWORK or the fully qualified '
            'subnetwork name. For more information, see '
            'https://cloud.google.com/compute/docs/vpc/'))
    parser.add_argument(
        '--worker_harness_container_image',
        default=None,
        help=('Docker registry location of container image to use for the '
              'worker harness. Default is the container for the version of the '
              'SDK. Note: currently, only approved Google Cloud Dataflow '
              'container images may be used here.'))
    parser.add_argument(
        '--use_public_ips',
        default=None,
        action='store_true',
        help='Whether to assign public IP addresses to the worker VMs.')
    parser.add_argument(
        '--no_use_public_ips',
        dest='use_public_ips',
        default=None,
        action='store_false',
        help='Whether to assign only private IP addresses to the worker VMs.')
    parser.add_argument(
        '--min_cpu_platform',
        dest='min_cpu_platform',
        type=str,
        help='GCE minimum CPU platform. Default is determined by GCP.'
    )
    parser.add_argument(
        '--dataflow_worker_jar',
        dest='dataflow_worker_jar',
        type=str,
        help='Dataflow worker jar file. If specified, the jar file is staged '
             'in GCS, then gets loaded by workers. End users usually '
             'should not use this feature.'
    )

  def validate(self, validator):
    errors = []
    if validator.is_service_runner():
      errors.extend(
          validator.validate_optional_argument_positive(self, 'num_workers'))
    return errors


class DebugOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--dataflow_job_file',
                        default=None,
                        help='Debug file to write the workflow specification.')
    parser.add_argument(
        '--experiment', '--experiments',
        dest='experiments',
        action='append',
        default=None,
        help=
        ('Runners may provide a number of experimental features that can be '
         'enabled with this flag. Please sync with the owners of the runner '
         'before enabling any experiments.'))

  def add_experiment(self, experiment):
    # pylint: disable=access-member-before-definition
    if self.experiments is None:
      self.experiments = []
    if experiment not in self.experiments:
      self.experiments.append(experiment)

  def lookup_experiment(self, key, default=None):
    if not self.experiments:
      return default
    elif key in self.experiments:
      return True
    for experiment in self.experiments:
      if experiment.startswith(key + '='):
        return experiment.split('=', 1)[1]
    return default


class ProfilingOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--profile_cpu',
                        action='store_true',
                        help='Enable work item CPU profiling.')
    parser.add_argument('--profile_memory',
                        action='store_true',
                        help='Enable work item heap profiling.')
    parser.add_argument('--profile_location',
                        default=None,
                        help='path for saving profiler data.')
    parser.add_argument('--profile_sample_rate',
                        type=float,
                        default=1.0,
                        help='A number between 0 and 1 indicating the ratio '
                        'of bundles that should be profiled.')


class SetupOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    # Options for installing dependencies in the worker.
    parser.add_argument(
        '--requirements_file',
        default=None,
        help=
        ('Path to a requirements file containing package dependencies. '
         'Typically it is produced by a pip freeze command. More details: '
         'https://pip.pypa.io/en/latest/reference/pip_freeze.html. '
         'If used, all the packages specified will be downloaded, '
         'cached (use --requirements_cache to change default location), '
         'and then staged so that they can be automatically installed in '
         'workers during startup. The cache is refreshed as needed '
         'avoiding extra downloads for existing packages. Typically the '
         'file is named requirements.txt.'))
    parser.add_argument(
        '--requirements_cache',
        default=None,
        help=
        ('Path to a folder to cache the packages specified in '
         'the requirements file using the --requirements_file option.'))
    parser.add_argument(
        '--setup_file',
        default=None,
        help=
        ('Path to a setup Python file containing package dependencies. If '
         'specified, the file\'s containing folder is assumed to have the '
         'structure required for a setuptools setup package. The file must be '
         'named setup.py. More details: '
         'https://pythonhosted.org/an_example_pypi_project/setuptools.html '
         'During job submission a source distribution will be built and the '
         'worker will install the resulting package before running any custom '
         'code.'))
    parser.add_argument(
        '--beam_plugin', '--beam_plugin',
        dest='beam_plugins',
        action='append',
        default=None,
        help=
        ('Bootstrap the python process before executing any code by importing '
         'all the plugins used in the pipeline. Please pass a comma separated'
         'list of import paths to be included. This is currently an '
         'experimental flag and provides no stability. Multiple '
         '--beam_plugin options can be specified if more than one plugin '
         'is needed.'))
    parser.add_argument(
        '--save_main_session',
        default=False,
        action='store_true',
        help=
        ('Save the main session state so that pickled functions and classes '
         'defined in __main__ (e.g. interactive session) can be unpickled. '
         'Some workflows do not need the session state if for instance all '
         'their functions/classes are defined in proper modules (not __main__)'
         ' and the modules are importable in the worker. '))
    parser.add_argument(
        '--sdk_location',
        default='default',
        help=
        ('Override the default location from where the Beam SDK is downloaded. '
         'It can be a URL, a GCS path, or a local path to an SDK tarball. '
         'Workflow submissions will download or copy an SDK tarball from here. '
         'If set to the string "default", a standard SDK location is used. If '
         'empty, no SDK is copied.'))
    parser.add_argument(
        '--extra_package', '--extra_packages',
        dest='extra_packages',
        action='append',
        default=None,
        help=
        ('Local path to a Python package file. The file is expected to be (1) '
         'a package tarball (".tar"), (2) a compressed package tarball '
         '(".tar.gz"), (3) a Wheel file (".whl") or (4) a compressed package '
         'zip file (".zip") which can be installed using the "pip install" '
         'command  of the standard pip package. Multiple --extra_package '
         'options can be specified if more than one package is needed. During '
         'job submission, the files will be staged in the staging area '
         '(--staging_location option) and the workers will install them in '
         'same order they were specified on the command line.'))


class PortableOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--job_endpoint',
                        default=None,
                        help=
                        ('Job service endpoint to use. Should be in the form '
                         'of address and port, e.g. localhost:3000'))
    parser.add_argument(
        '--environment_type', default=None,
        help=('Set the default environment type for running '
              'user code. Possible options are DOCKER and PROCESS.'))
    parser.add_argument(
        '--environment_config', default=None,
        help=('Set environment configuration for running the user code.\n For '
              'DOCKER: Url for the docker image.\n For PROCESS: json of the '
              'form {"os": "<OS>", "arch": "<ARCHITECTURE>", "command": '
              '"<process to execute>", "env":{"<Environment variables 1>": '
              '"<ENV_VAL>"} }. All fields in the json are optional except '
              'command.'))


class RunnerOptions(PipelineOptions):
  """Runner options are provided by the job service.

  The SDK has no a priori knowledge of runner options.
  It should be able to work with any portable runner.
  Runner specific options are discovered from the job service endpoint.
  """
  @classmethod
  def _add_argparse_args(cls, parser):
    # TODO: help option to display discovered options
    pass


class TestOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    # Options for e2e test pipeline.
    parser.add_argument(
        '--on_success_matcher',
        default=None,
        help=('Verify state/output of e2e test pipeline. This is pickled '
              'version of the matcher which should extends '
              'hamcrest.core.base_matcher.BaseMatcher.'))
    parser.add_argument(
        '--dry_run',
        default=False,
        help=('Used in unit testing runners without submitting the '
              'actual job.'))
    parser.add_argument(
        '--wait_until_finish_duration',
        default=None,
        type=int,
        help='The time to wait (in milliseconds) for test pipeline to finish. '
             'If it is set to None, it will wait indefinitely until the job '
             'is finished.')

  def validate(self, validator):
    errors = []
    if self.view_as(TestOptions).on_success_matcher:
      errors.extend(validator.validate_test_matcher(self, 'on_success_matcher'))
    return errors


class TestDataflowOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    # This option is passed to Dataflow Runner's Pub/Sub client. The camelCase
    # style in 'dest' matches the runner's.
    parser.add_argument(
        '--pubsub_root_url',
        dest='pubsubRootUrl',
        default=None,
        help='Root URL for use with the Google Cloud Pub/Sub API.',)


# TODO(silviuc): Add --files_to_stage option.
# This could potentially replace the --requirements_file and --setup_file.

# TODO(silviuc): Non-standard options. Keep them? If yes, add help too!
# Remote execution must check that this option is not None.


class OptionsContext(object):
  """Set default pipeline options for pipelines created in this block.

  This is particularly useful for pipelines implicitly created with the

      [python list] | PTransform

  construct.

  Can also be used as a decorator.
  """
  overrides = []

  def __init__(self, **options):
    self.options = options

  def __enter__(self):
    self.overrides.append(self.options)

  def __exit__(self, *exn_info):
    self.overrides.pop()

  def __call__(self, f, *args, **kwargs):

    def wrapper(*args, **kwargs):
      with self:
        f(*args, **kwargs)

    return wrapper

  @classmethod
  def augment_options(cls, options):
    for override in cls.overrides:
      for name, value in override.items():
        setattr(options, name, value)
    return options
