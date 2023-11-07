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

# pytype: skip-file

import argparse
import json
import logging
import os
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

import apache_beam as beam
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
    'AzureOptions',
    'HadoopFileSystemOptions',
    'WorkerOptions',
    'DebugOptions',
    'ProfilingOptions',
    'SetupOptions',
    'TestOptions',
    'S3Options'
]

PipelineOptionsT = TypeVar('PipelineOptionsT', bound='PipelineOptions')

_LOGGER = logging.getLogger(__name__)

# Map defined with option names to flag names for boolean options
# that have a destination(dest) in parser.add_argument() different
# from the flag name and whose default value is `None`.
_FLAG_THAT_SETS_FALSE_VALUE = {'use_public_ips': 'no_use_public_ips'}


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
        parser.add_value_provider_argument('--vp_arg1', default='start')
        parser.add_value_provider_argument('--vp_arg2')
        parser.add_argument('--non_vp_arg')

  """
  def add_value_provider_argument(self, *args, **kwargs):
    """ValueProvider arguments can be either of type keyword or positional.
    At runtime, even positional arguments will need to be supplied in the
    key/value form.
    """
    # Extract the option name from positional argument ['pos_arg']
    assert args and len(args[0]) >= 1
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
        default_value=default_value)

    # have add_argument do most of the work
    self.add_argument(*args, **kwargs)

  # The argparse package by default tries to autocomplete option names. This
  # results in an "ambiguous option" error from argparse when an unknown option
  # matching multiple known ones are used. This suppresses that behavior.
  def error(self, message):
    if message.startswith('ambiguous option: '):
      return
    super().error(message)


class _DictUnionAction(argparse.Action):
  """
  argparse Action take union of json loads values. If a key is specified in more
  than one of the values, the last value takes precedence.
  """
  def __call__(self, parser, namespace, values, option_string=None):
    if not hasattr(namespace,
                   self.dest) or getattr(namespace, self.dest) is None:
      setattr(namespace, self.dest, {})
    getattr(namespace, self.dest).update(values)


class PipelineOptions(HasDisplayData):
  """This class and subclasses are used as containers for command line options.

  These classes are wrappers over the standard argparse Python module
  (see https://docs.python.org/3/library/argparse.html).  To define one option
  or a group of options, create a subclass from PipelineOptions.

  Example Usage::

    class XyzOptions(PipelineOptions):

      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument('--abc', default='start')
        parser.add_argument('--xyz', default='end')

  The arguments for the add_argument() method are exactly the ones
  described in the argparse public documentation.

  Pipeline objects require an options object during initialization.
  This is obtained simply by initializing an options class as defined above.

  Example Usage::

    p = Pipeline(options=XyzOptions())
    if p.options.xyz == 'end':
      raise ValueError('Option xyz has an invalid value.')

  Instances of PipelineOptions or any of its subclass have access to values
  defined by other PipelineOption subclasses (see get_all_options()), and
  can be converted to an instance of another PipelineOptions subclass
  (see view_as()). All views share the underlying data structure that stores
  option key-value pairs.

  By default the options classes will use command line arguments to initialize
  the options.
  """
  def __init__(self, flags=None, **kwargs):
    # type: (Optional[List[str]], **Any) -> None

    """Initialize an options class.

    The initializer will traverse all subclasses, add all their argparse
    arguments and then parse the command line specified by flags or by default
    the one obtained from sys.argv.

    The subclasses of PipelineOptions do not need to redefine __init__.

    Args:
      flags: An iterable of command line arguments to be used. If not specified
        then sys.argv will be used as input for parsing arguments.

      **kwargs: Add overrides for arguments passed in flags. For overrides
                of arguments, please pass the `option names` instead of
                flag names.
                Option names: These are defined as dest in the
                parser.add_argument() for each flag. Passing flags
                like {no_use_public_ips: True}, for which the dest is
                defined to a different flag name in the parser,
                would be discarded. Instead, pass the dest of
                the flag (dest of no_use_public_ips is use_public_ips).
    """
    # Initializing logging configuration in case the user did not set it up.
    logging.basicConfig()

    # self._flags stores a list of not yet parsed arguments, typically,
    # command-line flags. This list is shared across different views.
    # See: view_as().
    self._flags = flags

    # Build parser that will parse options recognized by the [sub]class of
    # PipelineOptions whose object is being instantiated.
    parser = _BeamArgumentParser()
    for cls in type(self).mro():
      if cls == PipelineOptions:
        break
      elif '_add_argparse_args' in cls.__dict__:
        cls._add_argparse_args(parser)  # type: ignore

    # The _visible_options attribute will contain options that were recognized
    # by the parser.
    self._visible_options, _ = parser.parse_known_args(flags)

    # self._all_options is initialized with overrides to flag values,
    # provided in kwargs, and will store key-value pairs for options recognized
    # by current PipelineOptions [sub]class and its views that may be created.
    # See: view_as().
    # This dictionary is shared across different views, and is lazily updated
    # as each new views are created.
    # Users access this dictionary store via __getattr__ / __setattr__ methods.
    self._all_options = kwargs

    # Initialize values of keys defined by this class.
    for option_name in self._visible_option_list():
      # Note that options specified in kwargs will not be overwritten.
      if option_name not in self._all_options:
        self._all_options[option_name] = getattr(
            self._visible_options, option_name)

  @classmethod
  def _add_argparse_args(cls, parser):
    # type: (_BeamArgumentParser) -> None
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
      # Note: If a boolean flag is True in the dictionary,
      # implicitly the method assumes the boolean flag is
      # specified as a command line argument. If the
      # boolean flag is False, this method simply discards them.
      # Eg: {no_auth: True} is similar to python your_file.py --no_auth
      # {no_auth: False} is similar to python your_file.py.
      if isinstance(v, bool):
        if v:
          flags.append('--%s' % k)
        elif k in _FLAG_THAT_SETS_FALSE_VALUE:
          # Capture overriding flags, which have a different dest
          # from the flag name defined in the parser.add_argument
          # Eg: no_use_public_ips, which has the dest=use_public_ips
          # different from flag name
          flag_that_disables_the_option = (_FLAG_THAT_SETS_FALSE_VALUE[k])
          flags.append('--%s' % flag_that_disables_the_option)
      elif isinstance(v, list):
        for i in v:
          flags.append('--%s=%s' % (k, i))
      elif isinstance(v, dict):
        flags.append('--%s=%s' % (k, json.dumps(v)))
      elif v is None:
        # Don't process None type args here, they will be treated
        # as strings when parsed by BeamArgumentParser..
        logging.warning('Not setting flag with value None: %s', k)
      else:
        flags.append('--%s=%s' % (k, v))

    return cls(flags)

  def get_all_options(
      self,
      drop_default=False,
      add_extra_args_fn=None,  # type: Optional[Callable[[_BeamArgumentParser], None]]
      retain_unknown_options=False
  ):
    # type: (...) -> Dict[str, Any]

    """Returns a dictionary of all defined arguments.

    Returns a dictionary of all defined arguments (arguments that are defined in
    any subclass of PipelineOptions) into a dictionary.

    Args:
      drop_default: If set to true, options that are equal to their default
        values, are not returned as part of the result dictionary.
      add_extra_args_fn: Callback to populate additional arguments, can be used
        by runner to supply otherwise unknown args.
      retain_unknown_options: If set to true, options not recognized by any
        known pipeline options class will still be included in the result. If
        set to false, they will be discarded.

    Returns:
      Dictionary of all args and values.
    """

    # TODO(https://github.com/apache/beam/issues/18197): PipelineOption
    # sub-classes in the main session might be repeated. Pick last unique
    # instance of each subclass to avoid conflicts.
    subset = {}
    parser = _BeamArgumentParser()
    for cls in PipelineOptions.__subclasses__():
      subset[str(cls)] = cls
    for cls in subset.values():
      cls._add_argparse_args(parser)  # pylint: disable=protected-access
    if add_extra_args_fn:
      add_extra_args_fn(parser)

    known_args, unknown_args = parser.parse_known_args(self._flags)
    if retain_unknown_options:
      i = 0
      while i < len(unknown_args):
        # Treat all unary flags as booleans, and all binary argument values as
        # strings.
        if not unknown_args[i].startswith('-'):
          i += 1
          continue
        if i + 1 >= len(unknown_args) or unknown_args[i + 1].startswith('-'):
          split = unknown_args[i].split('=', 1)
          if len(split) == 1:
            parser.add_argument(unknown_args[i], action='store_true')
          else:
            parser.add_argument(split[0], type=str)
          i += 1
        elif unknown_args[i].startswith('--'):
          parser.add_argument(unknown_args[i], type=str)
          i += 2
        else:
          # skip all binary flags used with '-' and not '--'.
          # ex: using -f instead of --f (or --flexrs_goal) will prevent
          # argument validation before job submission and can be incorrectly
          # submitted to job.
          _LOGGER.warning(
              "Discarding flag %s, single dash flags are not allowed.",
              unknown_args[i])
          i += 2
          continue
      parsed_args, _ = parser.parse_known_args(self._flags)
    else:
      if unknown_args:
        _LOGGER.warning("Discarding unparseable args: %s", unknown_args)
      parsed_args = known_args
    result = vars(parsed_args)

    overrides = self._all_options.copy()
    # Apply the overrides if any
    for k in list(result):
      overrides.pop(k, None)
      if k in self._all_options:
        result[k] = self._all_options[k]
      if (drop_default and parser.get_default(k) == result[k] and
          not isinstance(parser.get_default(k), ValueProvider)):
        del result[k]

    if overrides:
      if retain_unknown_options:
        result.update(overrides)
      else:
        _LOGGER.warning("Discarding invalid overrides: %s", overrides)

    return result

  def display_data(self):
    return self.get_all_options(drop_default=True, retain_unknown_options=True)

  def view_as(self, cls):
    # type: (Type[PipelineOptionsT]) -> PipelineOptionsT

    """Returns a view of current object as provided PipelineOption subclass.

    Example Usage::

      options = PipelineOptions(['--runner', 'Direct', '--streaming'])
      standard_options = options.view_as(StandardOptions)
      if standard_options.streaming:
        # ... start a streaming job ...

    Note that options objects may have multiple views, and modifications
    of values in any view-object will apply to current object and other
    view-objects.

    Args:
      cls: PipelineOptions class or any of its subclasses.

    Returns:
      An instance of cls that is initialized using options contained in current
      object.

    """
    view = cls(self._flags)

    for option_name in view._visible_option_list():
      # Initialize values of keys defined by a cls.
      #
      # Note that we do initialization only once per key to make sure that
      # values in _all_options dict are not-recreated with each new view.
      # This is important to make sure that values of multi-options keys are
      # backed by the same list across multiple views, and that any overrides of
      # pipeline options already stored in _all_options are preserved.
      if option_name not in self._all_options:
        self._all_options[option_name] = getattr(
            view._visible_options, option_name)
    # Note that views will still store _all_options of the source object.
    view._all_options = self._all_options
    return view

  def _visible_option_list(self):
    # type: () -> List[str]
    return sorted(
        option for option in dir(self._visible_options) if option[0] != '_')

  def __dir__(self):
    # type: () -> List[str]
    return sorted(
        dir(type(self)) + list(self.__dict__) + self._visible_option_list())

  def __getattr__(self, name):
    # Special methods which may be accessed before the object is
    # fully constructed (e.g. in unpickling).
    if name[:2] == name[-2:] == '__':
      return object.__getattribute__(self, name)
    elif name in self._visible_option_list():
      return self._all_options[name]
    else:
      raise AttributeError(
          "'%s' object has no attribute '%s'" % (type(self).__name__, name))

  def __setattr__(self, name, value):
    if name in ('_flags', '_all_options', '_visible_options'):
      super().__setattr__(name, value)
    elif name in self._visible_option_list():
      self._all_options[name] = value
    else:
      raise AttributeError(
          "'%s' object has no attribute '%s'" % (type(self).__name__, name))

  def __str__(self):
    return '%s(%s)' % (
        type(self).__name__,
        ', '.join(
            '%s=%s' % (option, getattr(self, option))
            for option in self._visible_option_list()))


class StandardOptions(PipelineOptions):

  DEFAULT_RUNNER = 'DirectRunner'

  ALL_KNOWN_RUNNERS = (
      'apache_beam.runners.dataflow.dataflow_runner.DataflowRunner',
      'apache_beam.runners.direct.direct_runner.BundleBasedDirectRunner',
      'apache_beam.runners.direct.direct_runner.DirectRunner',
      'apache_beam.runners.direct.direct_runner.SwitchingDirectRunner',
      'apache_beam.runners.interactive.interactive_runner.InteractiveRunner',
      'apache_beam.runners.portability.flink_runner.FlinkRunner',
      'apache_beam.runners.portability.portable_runner.PortableRunner',
      'apache_beam.runners.portability.spark_runner.SparkRunner',
      'apache_beam.runners.test.TestDirectRunner',
      'apache_beam.runners.test.TestDataflowRunner',
  )

  KNOWN_RUNNER_NAMES = [path.split('.')[-1] for path in ALL_KNOWN_RUNNERS]

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--runner',
        help=(
            'Pipeline runner used to execute the workflow. Valid values are '
            'one of %s, or the fully qualified name of a PipelineRunner '
            'subclass. If unspecified, defaults to %s.' %
            (', '.join(cls.KNOWN_RUNNER_NAMES), cls.DEFAULT_RUNNER)))
    # Whether to enable streaming mode.
    parser.add_argument(
        '--streaming',
        default=False,
        action='store_true',
        help='Whether to enable streaming mode.')

    parser.add_argument(
        '--resource_hint',
        '--resource_hints',
        dest='resource_hints',
        action='append',
        default=[],
        help=(
            'Resource hint to set in the pipeline execution environment.'
            'Hints specified via this option override hints specified '
            'at transform level. Interpretation of hints is defined by '
            'Beam runners.'))

    parser.add_argument(
        '--auto_unique_labels',
        default=False,
        action='store_true',
        help='Whether to automatically generate unique transform labels '
        'for every transform. The default behavior is to raise an '
        'exception if a transform is created with a non-unique label.')


class CrossLanguageOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--beam_services',
        type=json.loads,
        default={},
        help=(
            'For convenience, Beam provides the ability to automatically '
            'download and start various services (such as expansion services) '
            'used at pipeline construction and execution. These services are '
            'identified by gradle target. This option provides the ability to '
            'use pre-started services or non-default pre-existing artifacts to '
            'start the given service. '
            'Should be a json mapping of gradle build targets to pre-built '
            'artifacts (e.g. jar files) expansion endpoints (e.g. host:port).'))

    parser.add_argument(
        '--use_transform_service',
        default=False,
        action='store_true',
        help='Use the Docker-composed-based transform service when expanding '
        'cross-language transforms.')


def additional_option_ptransform_fn():
  beam.transforms.ptransform.ptransform_fn_typehints_enabled = True


# Optional type checks that aren't enabled by default.
additional_type_checks = {
    'ptransform_fn': additional_option_ptransform_fn,
}  # type: Dict[str, Callable[[], None]]


def enable_all_additional_type_checks():
  """Same as passing --type_check_additional=all."""
  for f in additional_type_checks.values():
    f()


class TypeOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    # TODO(laolu): Add a type inferencing option here once implemented.
    parser.add_argument(
        '--type_check_strictness',
        default='DEFAULT_TO_ANY',
        choices=['ALL_REQUIRED', 'DEFAULT_TO_ANY'],
        help='The level of exhaustive manual type-hint '
        'annotation required')
    parser.add_argument(
        '--type_check_additional',
        default='',
        help='Comma separated list of additional type checking features to '
        'enable. Options: all, ptransform_fn. For details see:'
        'https://beam.apache.org/documentation/sdks/python-type-safety/')
    parser.add_argument(
        '--no_pipeline_type_check',
        dest='pipeline_type_check',
        action='store_false',
        help='Disable type checking at pipeline construction '
        'time')
    parser.add_argument(
        '--runtime_type_check',
        default=False,
        action='store_true',
        help='Enable type checking at pipeline execution '
        'time. NOTE: only supported with the '
        'DirectRunner')
    parser.add_argument(
        '--performance_runtime_type_check',
        default=False,
        action='store_true',
        help='Enable faster type checking via sampling at pipeline execution '
        'time. NOTE: only supported with portable runners '
        '(including the DirectRunner)')
    parser.add_argument(
        '--allow_non_deterministic_key_coders',
        default=False,
        action='store_true',
        help='Use non-deterministic coders (such as pickling) for key-grouping '
        'operations such as GroupByKey.  This is unsafe, as runners may group '
        'keys based on their encoded bytes, but is available for backwards '
        'compatibility. See BEAM-11719.')
    parser.add_argument(
        '--allow_unsafe_triggers',
        default=False,
        action='store_true',
        help='Allow the use of unsafe triggers. Unsafe triggers have the '
        'potential to cause data loss due to finishing and/or never having '
        'their condition met. Some operations, such as GroupByKey, disallow '
        'this. This exists for cases where such loss is acceptable and for '
        'backwards compatibility. See BEAM-9487.')

  def validate(self, unused_validator):
    errors = []
    if beam.version.__version__ >= '3':
      errors.append(
          'Update --type_check_additional default to include all '
          'available additional checks at Beam 3.0 release time.')
    keys = self.type_check_additional.split(',')

    for key in keys:
      if not key:
        continue
      elif key == 'all':
        enable_all_additional_type_checks()
      elif key in additional_type_checks:
        additional_type_checks[key]()
      else:
        errors.append('Unrecognized --type_check_additional feature: %s' % key)

    return errors


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
    parser.add_argument(
        '--direct_num_workers',
        type=int,
        default=1,
        help='number of parallel running workers.')
    parser.add_argument(
        '--direct_running_mode',
        default='in_memory',
        choices=['in_memory', 'multi_threading', 'multi_processing'],
        help='Workers running environment.')
    parser.add_argument(
        '--direct_embed_docker_python',
        default=False,
        action='store_true',
        dest='direct_embed_docker_python',
        help='DirectRunner uses the embedded Python environment when '
        'the default Python docker environment is specified.')
    parser.add_argument(
        '--direct_test_splits',
        default={},
        type=json.loads,
        help='Split test configuration of the json form '
        '{"step_name": {"timings": [...], "fractions": [...]}, ...} '
        'where step_name is the name of a step controlling the stage to which '
        'splits will be sent, timings is a list of floating-point times '
        '(in seconds) at which the split requests will be sent, and '
        'fractions is a corresponding list of floating points to use in the '
        'split requests themselves.')


class GoogleCloudOptions(PipelineOptions):
  """Google Cloud Dataflow service execution options."""

  BIGQUERY_API_SERVICE = 'bigquery.googleapis.com'
  COMPUTE_API_SERVICE = 'compute.googleapis.com'
  STORAGE_API_SERVICE = 'storage.googleapis.com'
  DATAFLOW_ENDPOINT = 'https://dataflow.googleapis.com'
  OAUTH_SCOPES = [
      'https://www.googleapis.com/auth/bigquery',
      'https://www.googleapis.com/auth/cloud-platform',
      'https://www.googleapis.com/auth/devstorage.full_control',
      'https://www.googleapis.com/auth/userinfo.email',
      'https://www.googleapis.com/auth/datastore',
      'https://www.googleapis.com/auth/spanner.admin',
      'https://www.googleapis.com/auth/spanner.data'
  ]

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--dataflow_endpoint',
        default=cls.DATAFLOW_ENDPOINT,
        help=(
            'The URL for the Dataflow API. If not set, the default public URL '
            'will be used.'))
    # Remote execution must check that this option is not None.
    parser.add_argument(
        '--project',
        default=None,
        help='Name of the Cloud project owning the Dataflow '
        'job.')
    # Remote execution must check that this option is not None.
    parser.add_argument(
        '--job_name', default=None, help='Name of the Cloud Dataflow job.')
    # Remote execution must check that this option is not None.
    parser.add_argument(
        '--staging_location',
        default=None,
        help='GCS path for staging code packages needed by '
        'workers.')
    # Remote execution must check that this option is not None.
    # If staging_location is not set, it defaults to temp_location.
    parser.add_argument(
        '--temp_location',
        default=None,
        help='GCS path for saving temporary workflow jobs.')
    # The Google Compute Engine region for creating Dataflow jobs. See
    # https://cloud.google.com/compute/docs/regions-zones/regions-zones for a
    # list of valid options.
    parser.add_argument(
        '--region',
        default=None,
        help='The Google Compute Engine region for creating '
        'Dataflow job.')
    parser.add_argument(
        '--service_account_email',
        default=None,
        help='Identity to run virtual machines as.')
    parser.add_argument(
        '--no_auth',
        dest='no_auth',
        action='store_true',
        default=False,
        help='Skips authorizing credentials with Google Cloud.')
    # Option to run templated pipelines
    parser.add_argument(
        '--template_location',
        default=None,
        help='Save job to specified local or GCS location.')
    parser.add_argument(
        '--label',
        '--labels',
        dest='labels',
        action='append',
        default=None,
        help='Labels to be applied to this Dataflow job. '
        'Labels are key value pairs separated by = '
        '(e.g. --label key=value) or '
        '(--labels=\'{ "key": "value", "mass": "1_3kg", "count": "3" }\').')
    parser.add_argument(
        '--update',
        default=False,
        action='store_true',
        help='Update an existing streaming Cloud Dataflow job. '
        'See https://cloud.google.com/dataflow/docs/guides/'
        'updating-a-pipeline')
    parser.add_argument(
        '--transform_name_mapping',
        default=None,
        type=json.loads,
        help='The transform mapping that maps the named '
        'transforms in your prior pipeline code to names '
        'in your replacement pipeline code.'
        'See https://cloud.google.com/dataflow/docs/guides/'
        'updating-a-pipeline')
    parser.add_argument(
        '--enable_streaming_engine',
        default=False,
        action='store_true',
        help='Enable Windmill Service for this Dataflow job. ')
    parser.add_argument(
        '--dataflow_kms_key',
        default=None,
        help='Set a Google Cloud KMS key name to be used in '
        'Dataflow state operations (GBK, Streaming).')
    parser.add_argument(
        '--create_from_snapshot',
        default=None,
        help='The snapshot from which the job should be created.')
    parser.add_argument(
        '--flexrs_goal',
        default=None,
        choices=['COST_OPTIMIZED', 'SPEED_OPTIMIZED'],
        help='Set the Flexible Resource Scheduling mode')
    parser.add_argument(
        '--dataflow_service_option',
        '--dataflow_service_options',
        dest='dataflow_service_options',
        action='append',
        default=None,
        help=(
            'Options to configure the Dataflow service. These '
            'options decouple service side feature availability '
            'from the Apache Beam release cycle.'
            'Note: If set programmatically, must be set as a '
            'list of strings'))
    parser.add_argument(
        '--enable_hot_key_logging',
        default=False,
        action='store_true',
        help='When true, will enable the direct logging of any detected hot '
        'keys into Cloud Logging. Warning: this will log the literal key as an '
        'unobfuscated string.')
    parser.add_argument(
        '--enable_artifact_caching',
        default=False,
        action='store_true',
        help='When true, artifacts will be cached across job submissions in '
        'the GCS staging bucket')
    parser.add_argument(
        '--impersonate_service_account',
        default=None,
        help='All API requests will be made as the given service account or '
        'target service account in an impersonation delegation chain '
        'instead of the currently selected account. You can specify '
        'either a single service account as the impersonator, or a '
        'comma-separated list of service accounts to create an '
        'impersonation delegation chain.')
    parser.add_argument(
        '--gcp_oauth_scope',
        '--gcp_oauth_scopes',
        dest='gcp_oauth_scopes',
        action='append',
        default=cls.OAUTH_SCOPES,
        help=(
            'Controls the OAuth scopes that will be requested when creating '
            'GCP credentials. Note: If set programmatically, must be set as a '
            'list of strings'))

  def _create_default_gcs_bucket(self):
    try:
      from apache_beam.io.gcp import gcsio
    except ImportError:
      _LOGGER.warning('Unable to create default GCS bucket.')
      return None
    bucket = gcsio.get_or_create_default_gcs_bucket(self)
    if bucket:
      return 'gs://%s' % bucket.id
    else:
      return None

  # If either temp or staging location has an issue, we use the valid one for
  # both locations. If both are bad we return an error.
  def _handle_temp_and_staging_locations(self, validator):
    temp_errors = validator.validate_gcs_path(self, 'temp_location')
    staging_errors = validator.validate_gcs_path(self, 'staging_location')
    if temp_errors and not staging_errors:
      setattr(self, 'temp_location', getattr(self, 'staging_location'))
      return []
    elif staging_errors and not temp_errors:
      setattr(self, 'staging_location', getattr(self, 'temp_location'))
      return []
    elif not staging_errors and not temp_errors:
      return []
    # Both staging and temp locations are bad, try to use default bucket.
    else:
      default_bucket = self._create_default_gcs_bucket()
      if default_bucket is None:
        temp_errors.extend(staging_errors)
        return temp_errors
      else:
        setattr(self, 'temp_location', default_bucket)
        setattr(self, 'staging_location', default_bucket)
        return []

  def validate(self, validator):
    errors = []
    if validator.is_service_runner():
      errors.extend(self._handle_temp_and_staging_locations(validator))
      errors.extend(validator.validate_cloud_options(self))

    if self.view_as(DebugOptions).dataflow_job_file:
      if self.view_as(GoogleCloudOptions).template_location:
        errors.append(
            '--dataflow_job_file and --template_location '
            'are mutually exclusive.')

    # Validate that dataflow_service_options is a list
    if self.dataflow_service_options:
      errors.extend(
          validator.validate_repeatable_argument_passed_as_list(
              self, 'dataflow_service_options'))

    return errors

  def get_cloud_profiler_service_name(self):
    _ENABLE_GOOGLE_CLOUD_PROFILER = 'enable_google_cloud_profiler'
    if self.dataflow_service_options:
      if _ENABLE_GOOGLE_CLOUD_PROFILER in self.dataflow_service_options:
        return os.environ["JOB_NAME"]
      for option_name in self.dataflow_service_options:
        if option_name.startswith(_ENABLE_GOOGLE_CLOUD_PROFILER + '='):
          return option_name.split('=', 1)[1]

    experiments = self.view_as(DebugOptions).experiments or []
    if _ENABLE_GOOGLE_CLOUD_PROFILER in experiments:
      return os.environ["JOB_NAME"]

    return None


class AzureOptions(PipelineOptions):
  """Azure Blob Storage options."""
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--azure_connection_string',
        default=None,
        help='Connection string of the Azure Blob Storage Account.')
    parser.add_argument(
        '--blob_service_endpoint',
        default=None,
        help='URL of the Azure Blob Storage Account.')
    parser.add_argument(
        '--azure_managed_identity_client_id',
        default=None,
        help='Client ID of a user-assigned managed identity.')

  def validate(self, validator):
    errors = []
    if self.azure_connection_string:
      if self.blob_service_endpoint:
        errors.append(
            '--azure_connection_string and '
            '--blob_service_endpoint are mutually exclusive.')

    return errors


class HadoopFileSystemOptions(PipelineOptions):
  """``HadoopFileSystem`` connection options."""
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--hdfs_host',
        default=None,
        help=('Hostname or address of the HDFS namenode.'))
    parser.add_argument(
        '--hdfs_port', default=None, help=('Port of the HDFS namenode.'))
    parser.add_argument(
        '--hdfs_user', default=None, help=('HDFS username to use.'))
    parser.add_argument(
        '--hdfs_full_urls',
        default=False,
        action='store_true',
        help=(
            'If set, URLs will be parsed as "hdfs://server/path/...", instead '
            'of "hdfs://path/...". The "server" part will be unused (use '
            '--hdfs_host and --hdfs_port).'))

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
        help=(
            'Number of workers to use when executing the Dataflow job. If not '
            'set, the Dataflow service will use a reasonable default.'))
    parser.add_argument(
        '--max_num_workers',
        type=int,
        default=None,
        help=(
            'Maximum number of workers to use when executing the Dataflow job.'
        ))
    parser.add_argument(
        '--autoscaling_algorithm',
        type=str,
        choices=['NONE', 'THROUGHPUT_BASED'],
        default=None,  # Meaning unset, distinct from 'NONE' meaning don't scale
        help=
        ('If and how to autoscale the workerpool.'))
    parser.add_argument(
        '--worker_machine_type',
        '--machine_type',
        dest='machine_type',
        default=None,
        help=(
            'Machine type to create Dataflow worker VMs as. See '
            'https://cloud.google.com/compute/docs/machine-types '
            'for a list of valid options. If not set, '
            'the Dataflow service will choose a reasonable '
            'default.'))
    parser.add_argument(
        '--disk_size_gb',
        type=int,
        default=None,
        help=(
            'Remote worker disk size, in gigabytes, or 0 to use the default '
            'size. If not set, the Dataflow service will use a reasonable '
            'default.'))
    parser.add_argument(
        '--worker_disk_type',
        '--disk_type',
        dest='disk_type',
        default=None,
        help=('Specifies what type of persistent disk should be used.'))
    parser.add_argument(
        '--worker_region',
        default=None,
        help=(
            'The Compute Engine region (https://cloud.google.com/compute/docs/'
            'regions-zones/regions-zones) in which worker processing should '
            'occur, e.g. "us-west1". Mutually exclusive with worker_zone. If '
            'neither worker_region nor worker_zone is specified, default to '
            'same value as --region.'))
    parser.add_argument(
        '--worker_zone',
        default=None,
        help=(
            'The Compute Engine zone (https://cloud.google.com/compute/docs/'
            'regions-zones/regions-zones) in which worker processing should '
            'occur, e.g. "us-west1-a". Mutually exclusive with worker_region. '
            'If neither worker_region nor worker_zone is specified, the '
            'Dataflow service will choose a zone in --region based on '
            'available capacity.'))
    parser.add_argument(
        '--zone',
        default=None,
        help=(
            'GCE availability zone for launching workers. Default is up to the '
            'Dataflow service. This flag is deprecated, and will be replaced '
            'by worker_zone.'))
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
        help=(
            'Docker registry location of container image to use for the '
            'worker harness. If not set, an appropriate approved Google Cloud '
            'Dataflow image will be used based on the version of the '
            'SDK. Note: This flag is deprecated and only supports '
            'approved Google Cloud Dataflow container images. To provide a '
            'custom container image, use sdk_container_image instead.'))
    parser.add_argument(
        '--sdk_container_image',
        default=None,
        help=(
            'Docker registry location of container image to use for the '
            'worker harness. If not set, an appropriate approved Google Cloud '
            'Dataflow image will be used based on the version of the '
            'SDK. If set for a non-portable pipeline, only official '
            'Google Cloud Dataflow container images may be used here.'))
    parser.add_argument(
        '--sdk_harness_container_image_overrides',
        action='append',
        default=None,
        help=(
            'Overrides for SDK harness container images. Could be for the '
            'local SDK or for a remote SDK that pipeline has to support due '
            'to a cross-language transform. Each entry consist of two values '
            'separated by a comma where first value gives a regex to '
            'identify the container image to override and the second value '
            'gives the replacement container image.'))
    parser.add_argument(
        '--default_sdk_harness_log_level',
        default=None,
        help=(
            'Controls the default log level of all loggers without a log level '
            'override. Values can be either a labeled level or a number '
            '(See https://docs.python.org/3/library/logging.html#levels). '
            'Default log level is INFO.'))
    parser.add_argument(
        '--sdk_harness_log_level_overrides',
        type=json.loads,
        action=_DictUnionAction,
        default=None,
        help=(
            'Controls the log levels for specifically named loggers. The '
            'expected format is a json string: \'{"module":"log_level",...}\'. '
            'For example, by specifying the value \'{"a.b.c":"DEBUG"}\', '
            'the logger underneath the module "a.b.c" will be configured to '
            'output logs at the DEBUG level. Similarly, by specifying the '
            'value \'{"a.b.c":"WARNING"}\' all loggers underneath the "a.b.c" '
            'module will be configured to output logs at the WARNING level. '
            'Also, note that when multiple overrides are specified, the exact '
            'name followed by the closest parent takes precedence.'))
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
        help='GCE minimum CPU platform. Default is determined by GCP.')
    parser.add_argument(
        '--max_cache_memory_usage_mb',
        dest='max_cache_memory_usage_mb',
        type=int,
        default=100,
        help=(
            'Size of the SDK Harness cache to store user state and side '
            'inputs in MB. Default is 100MB. If the cache is full, least '
            'recently used elements will be evicted. This cache is per '
            'each SDK Harness instance. SDK Harness is a component '
            'responsible for executing the user code and communicating with '
            'the runner. Depending on the runner, there may be more than one '
            'SDK Harness process running on the same worker node. Increasing '
            'cache size might improve performance of some pipelines, but can '
            'lead to an increase in memory consumption and OOM errors if '
            'workers are not appropriately provisioned.'))

  def validate(self, validator):
    errors = []
    errors.extend(validator.validate_sdk_container_image_options(self))

    if validator.is_service_runner():
      errors.extend(validator.validate_num_workers(self))
      errors.extend(validator.validate_worker_region_zone(self))
    return errors


class DebugOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--dataflow_job_file',
        default=None,
        help='Debug file to write the workflow specification.')
    parser.add_argument(
        '--experiment',
        '--experiments',
        dest='experiments',
        action='append',
        default=None,
        help=(
            'Runners may provide a number of experimental features that can be '
            'enabled with this flag. Please sync with the owners of the runner '
            'before enabling any experiments.'))

    parser.add_argument(
        '--number_of_worker_harness_threads',
        type=int,
        default=None,
        help=(
            'Number of threads per worker to use on the runner. If left '
            'unspecified, the runner will compute an appropriate number of '
            'threads to use.'))

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

  def validate(self, validator):
    errors = []
    if self.experiments:
      errors.extend(
          validator.validate_repeatable_argument_passed_as_list(
              self, 'experiments'))
    return errors


class ProfilingOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--profile_cpu',
        action='store_true',
        help='Enable work item CPU profiling.')
    parser.add_argument(
        '--profile_memory',
        action='store_true',
        help='Enable work item heap profiling.')
    parser.add_argument(
        '--profile_location',
        default=None,
        help='path for saving profiler data.')
    parser.add_argument(
        '--profile_sample_rate',
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
        help=(
            'Path to a requirements file containing package dependencies. '
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
        help=(
            'Path to a folder to cache the packages specified in '
            'the requirements file using the --requirements_file option.'
            'If you want to skip populating requirements cache, please '
            'specify --requirements_cache="skip".'))
    parser.add_argument(
        '--requirements_cache_only_sources',
        action='store_true',
        help=(
            'Enable this flag to populate requirements cache only '
            'with Source distributions(sdists) of the dependencies '
            'mentioned in the --requirements_file'
            'Note: (BEAM-4032): This flag may significantly slow down '
            'the pipeline submission. It is added to preserve the requirements'
            ' cache behavior prior to 2.37.0 and will likely be removed in '
            'future releases.'))
    parser.add_argument(
        '--setup_file',
        default=None,
        help=(
            'Path to a setup Python file containing package dependencies. If '
            'specified, the file\'s containing folder is assumed to have the '
            'structure required for a setuptools setup package. The file must '
            'be named setup.py. More details: '
            'https://pythonhosted.org/an_example_pypi_project/setuptools.html '
            'During job submission a source distribution will be built and '
            'the worker will install the resulting package before running any '
            'custom code.'))
    parser.add_argument(
        '--beam_plugin',
        '--beam_plugins',
        dest='beam_plugins',
        action='append',
        default=None,
        help=(
            'Bootstrap the python process before executing any code by '
            'importing all the plugins used in the pipeline. Please pass a '
            'comma separated list of import paths to be included. This is '
            'currently an experimental flag and provides no stability. '
            'Multiple --beam_plugin options can be specified if more than '
            'one plugin is needed.'))
    parser.add_argument(
        '--pickle_library',
        default='default',
        help=(
            'Chooses which pickle library to use. Options are dill, '
            'cloudpickle or default.'),
        choices=['cloudpickle', 'default', 'dill'])
    parser.add_argument(
        '--save_main_session',
        default=False,
        action='store_true',
        help=(
            'Save the main session state so that pickled functions and classes '
            'defined in __main__ (e.g. interactive session) can be unpickled. '
            'Some workflows do not need the session state if for instance all '
            'their functions/classes are defined in proper modules '
            '(not __main__) and the modules are importable in the worker. '))
    parser.add_argument(
        '--sdk_location',
        default='default',
        help=(
            'Path to a custom Beam SDK package to install and use on the'
            'runner. It can be a URL, a GCS path, or a local path to an '
            'SDK tarball. Workflow submissions will download or copy an SDK '
            'tarball from here. If set to "default", '
            'runners will use the SDK provided in the default environment.'
            'Use this flag when running pipelines with an unreleased or '
            'manually patched version of Beam SDK.'))
    parser.add_argument(
        '--extra_package',
        '--extra_packages',
        dest='extra_packages',
        action='append',
        default=None,
        help=(
            'Local path to a Python package file. The file is expected to be '
            '(1) a package tarball (".tar"), (2) a compressed package tarball '
            '(".tar.gz"), (3) a Wheel file (".whl") or (4) a compressed '
            'package zip file (".zip") which can be installed using the '
            '"pip install" command  of the standard pip package. Multiple '
            '--extra_package options can be specified if more than one '
            'package is needed. During job submission, the files will be '
            'staged in the staging area (--staging_location option) and the '
            'workers will install them in same order they were specified on '
            'the command line.'))
    parser.add_argument(
        '--prebuild_sdk_container_engine',
        help=(
            'Prebuild sdk worker container image before job submission. If '
            'enabled, SDK invokes the boot sequence in SDK worker '
            'containers to install all pipeline dependencies in the '
            'container, and uses the prebuilt image in the pipeline '
            'environment. This may speed up pipeline execution. To enable, '
            'select the Docker build engine: local_docker using '
            'locally-installed Docker or cloud_build for using Google Cloud '
            'Build (requires a GCP project with Cloud Build API enabled). You '
            'can also subclass SdkContainerImageBuilder and use that to build '
            'in other environments.'))
    parser.add_argument(
        '--prebuild_sdk_container_base_image',
        default=None,
        help=('Deprecated. Use --sdk_container_image instead.'))
    parser.add_argument(
        '--cloud_build_machine_type',
        default=None,
        help=(
            'If specified, use the machine type explicitly when prebuilding'
            'SDK container image on Google Cloud Build.'))
    parser.add_argument(
        '--docker_registry_push_url',
        default=None,
        help=(
            'Docker registry url to use for tagging and pushing the prebuilt '
            'sdk worker container image.'))

  def validate(self, validator):
    errors = []
    errors.extend(validator.validate_container_prebuilding_options(self))
    return errors


class PortableOptions(PipelineOptions):
  """Portable options are common options expected to be understood by most of
  the portable runners. Should generally be kept in sync with
  PortablePipelineOptions.java.
  """
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--job_endpoint',
        default=None,
        help=(
            'Job service endpoint to use. Should be in the form of host '
            'and port, e.g. localhost:8099.'))
    parser.add_argument(
        '--artifact_endpoint',
        default=None,
        help=(
            'Artifact staging endpoint to use. Should be in the form of host '
            'and port, e.g. localhost:8098. If none is specified, the '
            'artifact endpoint sent from the job server is used.'))
    parser.add_argument(
        '--job_server_timeout',
        '--job-server-timeout',  # For backwards compatibility.
        default=60,
        type=int,
        help=(
            'Job service request timeout in seconds. The timeout '
            'determines the max time the driver program will wait to '
            'get a response from the job server. NOTE: the timeout does not '
            'apply to the actual pipeline run time. The driver program can '
            'still wait for job completion indefinitely.'))
    parser.add_argument(
        '--environment_type',
        default=None,
        help=(
            'Set the default environment type for running '
            'user code. DOCKER (default) runs user code in a container. '
            'PROCESS runs user code in processes that are automatically '
            'started on each worker node. LOOPBACK runs user code on the '
            'same process that originally submitted the job.'))
    parser.add_argument(
        '--environment_config',
        default=None,
        help=(
            'Set environment configuration for running the user code.\n For '
            'DOCKER: Url for the docker image.\n For PROCESS: json of the '
            'form {"os": "<OS>", "arch": "<ARCHITECTURE>", "command": '
            '"<process to execute>", "env":{"<Environment variables 1>": '
            '"<ENV_VAL>"} }. All fields in the json are optional except '
            'command.\n\nPrefer using --environment_options instead.'))
    parser.add_argument(
        '--environment_option',
        '--environment_options',
        dest='environment_options',
        action='append',
        default=None,
        help=(
            'Environment configuration for running the user code. '
            'Recognized options depend on --environment_type.\n '
            'For DOCKER: docker_container_image (optional)\n '
            'For PROCESS: process_command (required), process_variables '
            '(optional, comma-separated)\n '
            'For EXTERNAL: external_service_address (required)'))
    parser.add_argument(
        '--sdk_worker_parallelism',
        default=1,
        help=(
            'Sets the number of sdk worker processes that will run on each '
            'worker node. Default is 1. If 0, a value will be chosen by the '
            'runner.'))
    parser.add_argument(
        '--environment_cache_millis',
        default=0,
        help=(
            'Duration in milliseconds for environment cache within a job. '
            '0 means no caching.'))
    parser.add_argument(
        '--output_executable_path',
        default=None,
        help=(
            'Create an executable jar at this path rather than running '
            'the pipeline.'))

  def validate(self, validator):
    return validator.validate_environment_options(self)

  def add_environment_option(self, option):
    # pylint: disable=access-member-before-definition
    if self.environment_options is None:
      self.environment_options = []
    if option not in self.environment_options:
      self.environment_options.append(option)

  def lookup_environment_option(self, key, default=None):
    if not self.environment_options:
      return default
    elif key in self.environment_options:
      return True
    for option in self.environment_options:
      if option.startswith(key + '='):
        return option.split('=', 1)[1]
    return default


class JobServerOptions(PipelineOptions):
  """Options for starting a Beam job server. Roughly corresponds to
  JobServerDriver.ServerConfiguration in Java.
  """
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--artifacts_dir',
        default=None,
        help='The location to store staged artifact files. '
        'Any Beam-supported file system is allowed. '
        'If unset, the local temp dir will be used.')
    parser.add_argument(
        '--job_port',
        default=0,
        type=int,
        help='Port to use for the job service. 0 to use a '
        'dynamic port.')
    parser.add_argument(
        '--artifact_port',
        default=0,
        type=int,
        help='Port to use for artifact staging. 0 to use a '
        'dynamic port.')
    parser.add_argument(
        '--expansion_port',
        default=0,
        type=int,
        help='Port to use for artifact staging. 0 to use a '
        'dynamic port.')
    parser.add_argument(
        '--job_server_java_launcher',
        default='java',
        help='The Java Application Launcher executable file to use for '
        'starting a Java job server. If unset, `java` from the '
        'environment\'s $PATH is used.')
    parser.add_argument(
        '--job_server_jvm_properties',
        '--job_server_jvm_property',
        dest='job_server_jvm_properties',
        action='append',
        default=[],
        help='JVM properties to pass to a Java job server.')


class FlinkRunnerOptions(PipelineOptions):

  # These should stay in sync with gradle.properties.
  PUBLISHED_FLINK_VERSIONS = ['1.12', '1.13', '1.14', '1.15', '1.16']

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--flink_master',
        default='[auto]',
        help='Flink master address (http://host:port)'
        ' Use "[local]" to start a local cluster'
        ' for the execution. Use "[auto]" if you'
        ' plan to either execute locally or let the'
        ' Flink job server infer the cluster address.')
    parser.add_argument(
        '--flink_version',
        default=cls.PUBLISHED_FLINK_VERSIONS[-1],
        choices=cls.PUBLISHED_FLINK_VERSIONS,
        help='Flink version to use.')
    parser.add_argument(
        '--flink_job_server_jar', help='Path or URL to a flink jobserver jar.')
    parser.add_argument(
        '--flink_submit_uber_jar',
        default=False,
        action='store_true',
        help='Create and upload an uberjar to the flink master'
        ' directly, rather than starting up a job server.'
        ' Only applies when flink_master is set to a'
        ' cluster address.  Requires Python 3.6+.')
    parser.add_argument(
        '--parallelism',
        type=int,
        default=-1,
        help='The degree of parallelism to be used when distributing'
        ' operations onto workers. If the parallelism is not set, the'
        ' configured Flink default is used, or 1 if none can be found.')
    parser.add_argument(
        '--max_parallelism',
        type=int,
        default=-1,
        help='The pipeline wide maximum degree of parallelism to be used. The'
        ' maximum parallelism specifies the upper limit for dynamic scaling'
        ' and the number of key groups used for partitioned state.')


class SparkRunnerOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--spark_master_url',
        default='local[4]',
        help='Spark master URL (spark://HOST:PORT). '
        'Use "local" (single-threaded) or "local[*]" '
        '(multi-threaded) to start a local cluster for '
        'the execution.')
    parser.add_argument(
        '--spark_job_server_jar',
        help='Path or URL to a Beam Spark job server jar. '
        'Overrides --spark_version.')
    parser.add_argument(
        '--spark_submit_uber_jar',
        default=False,
        action='store_true',
        help='Create and upload an uber jar to the Spark REST'
        ' endpoint, rather than starting up a job server.'
        ' Requires Python 3.6+.')
    parser.add_argument(
        '--spark_rest_url',
        help='URL for the Spark REST endpoint. '
        'Only required when using spark_submit_uber_jar. '
        'For example, http://hostname:6066')
    parser.add_argument(
        '--spark_version',
        default='3',
        choices=['3'],
        help='Spark major version to use.')


class TestOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    # Options for e2e test pipeline.
    parser.add_argument(
        '--on_success_matcher',
        default=None,
        help=(
            'Verify state/output of e2e test pipeline. This is pickled '
            'version of the matcher which should extends '
            'hamcrest.core.base_matcher.BaseMatcher.'))
    parser.add_argument(
        '--dry_run',
        default=False,
        help=(
            'Used in unit testing runners without submitting the '
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
        help='Root URL for use with the Google Cloud Pub/Sub API.',
    )


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
  overrides = []  # type: List[Dict[str, Any]]

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


class S3Options(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    # These options are passed to the S3 IO Client
    parser.add_argument(
        '--s3_access_key_id',
        default=None,
        help='The secret key to use when creating the s3 client.')
    parser.add_argument(
        '--s3_secret_access_key',
        default=None,
        help='The secret key to use when creating the s3 client.')
    parser.add_argument(
        '--s3_session_token',
        default=None,
        help='The session token to use when creating the s3 client.')
    parser.add_argument(
        '--s3_endpoint_url',
        default=None,
        help='The complete URL to use for the constructed s3 client.')
    parser.add_argument(
        '--s3_region_name',
        default=None,
        help='The name of the region associated with the s3 client.')
    parser.add_argument(
        '--s3_api_version',
        default=None,
        help='The API version to use with the s3 client.')
    parser.add_argument(
        '--s3_verify',
        default=None,
        help='Whether or not to verify SSL certificates with the s3 client.')
    parser.add_argument(
        '--s3_disable_ssl',
        default=False,
        action='store_true',
        help=(
            'Whether or not to use SSL with the s3 client. '
            'By default, SSL is used.'))
