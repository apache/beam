# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pipeline options obtained from command line parsing.

TODO(silviuc): Should rename this module to pipeline_options.
"""

import argparse
import sys

# Raw (unparsed) options. They are also added by other modules that want to
# contribute modules other than the ones defined in this file. See add_option(),
# below.
OPTIONS = []


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


def add_option(*args, **kwargs):
  """Adds an option using the syntax of the standard argparse module.

  Function can be called by any module that wants to add command line options
  to be parsed during execution.
  """
  # TODO(silviuc): Should do some checking on proper format eventually.
  OPTIONS.append((args, kwargs))


def get_options(args=None):
  """Returns a parsed arguments object based on command line arguments.

  The function will parse the arguments according to the argument definitions
  registered by calls to add_option().

  Args:
    args: A list of arguments as if passed through command line. If None then
      the real command line is used (i.e., sys.argv).

  Returns:
    A parsed arguments object returned by an ArgumentParser object.
  """
  parser = argparse.ArgumentParser()
  for opt_args, opt_kwargs in OPTIONS:
    parser.add_argument(*opt_args, **opt_kwargs)
  # We have both a positive and negative flag, set the default after they're
  # both registered.
  parser.set_defaults(pipeline_type_check=True)
  return OptionsContext.augment_options(
      parser.parse_args(args if args is not None else sys.argv[1:]))


# TODO(laolu): Add a type inferencing option here once implemented.
add_option(
    '--type_check_strictness', dest='type_check_strictness',
    default='DEFAULT_TO_ANY',
    choices=['ALL_REQUIRED', 'DEFAULT_TO_ANY'],
    help='The level of exhaustive manual type-hint annotation required')

add_option(
    '--no_pipeline_type_check', dest='pipeline_type_check',
    action='store_false',
    help='Disable type checking at pipeline construction time')

add_option(
    '--pipeline_type_check', dest='pipeline_type_check',
    action='store_true',
    help='Enable type checking at pipeline construction time')

add_option(
    '--runtime_type_check', dest='runtime_type_check',
    default=False, action='store_true',
    help='Enable type checking at pipeline execution time. NOTE: only supported'
    ' with the DirectPipelineRunner')

add_option(
    '--dataflow_endpoint', dest='dataflow_endpoint',
    default='https://dataflow.googleapis.com',
    help=('The URL for the Dataflow API. If not set, '
          'the default public URL will be used.'))

add_option(
    '--dataflow_job_file', dest='dataflow_job_file', default=None,
    help='Debug file to write the workflow specification.')

# Remote execution must check that this option is not None.
add_option(
    '--job_name', dest='job_name', default=None,
    help='Name of the Cloud Dataflow job.')

# Remote execution must check that this option is not None.
add_option(
    '--project', dest='project', default=None,
    help='Name of the Cloud project owning the Dataflow job.')

add_option(
    '--runner', dest='runner', default='DirectPipelineRunner',
    help=('Pipeline runner used to execute the workflow. '
          'Valid values are DirectPipelineRunner, DataflowPipelineRunner, '
          'and BlockingDataflowPipelineRunner.'))

# Whether this is a streaming job.
# TODO(ccy): This should be an option on the pipeline runner.
add_option('--is_streaming',
           dest='is_streaming',
           default=False,
           help='Whether the job is a streaming job.')

# Remote execution must check that this option is not None.
add_option(
    '--staging_location', dest='staging_location', default=None,
    help='GCS path for staging code packages needed by workers.')

# Remote execution must check that this option is not None.
add_option(
    '--temp_location', dest='temp_location', default=None,
    help='GCS path for saving temporary workflow jobs.')

# Options for using service account credentials.
add_option(
    '--service_account_name', dest='service_account_name', default=None,
    help='Name of the service account for Google APIs.')

add_option(
    '--service_account_key_file', dest='service_account_key_file',
    default=None,
    help='Path to a file containing the P12 service credentials.')

# Options for installing dependencies in the worker.
add_option(
    '--requirements_file', dest='requirements_file',
    default=None,
    help=('Path to a requirements file containing package dependencies. '
          'Typically it is produced by a pip freeze command. More details: '
          'https://pip.pypa.io/en/latest/reference/pip_freeze.html '
          'If specified, the worker will install the required dependenciesi '
          'before running any custom code. Typically the file is named '
          'requirements.txt.'))

add_option(
    '--setup_file', dest='setup_file',
    default=None,
    help=('Path to a setup Python file containing package dependencies. '
          'If specified, the file\'s containing folder is assumed to have the '
          'structure required for a setuptools setup package. '
          'The file must be named setup.py. '
          'More details: https://pythonhosted.org/setuptools/setuptools.html '
          'During job submission a source distribution will be built and the '
          'worker will install the resulting package before running '
          'any custom code.'))

add_option(
    '--extra_package', dest='extra_packages', action='append', default=None,
    help=('Local path to a Python package file. The file is expected to be a '
          'compressed tarball with the suffix \'.tar.gz\' '
          'which can be installed using the easy_install command '
          'of the standard setuptools package. '
          'Multiple --extra_package options can be specified if more than one '
          'package is needed. '
          'During job submission the files will be staged in the staging '
          'area (--staging_location option) and the workers will install them '
          'in same order they were specified on the command line.'))

add_option(
    '--save_main_session', dest='save_main_session',
    default=True, action='store_true',
    help=('Save the main session state so that pickled functions and classes '
          'defined in __main__ (e.g. interactive session) can be unpickled. '
          'Some workflows do not need the session state if for instance all '
          'their functions/classes are defined in proper modules '
          '(not __main__) and the modules are importable in the worker. '))
add_option(
    '--no_save_main_session', dest='save_main_session', action='store_false')

add_option(
    '--sdk_location', dest='sdk_location',
    default='default',
    help=('GCS folder or local directory containing the Dataflow SDK '
          'for Python tar file. '
          'Workflow submissions will copy an SDK tarball from here. '
          'If this is a GCS folder, the '
          'corresponding SDK version will be added to the tar file name. '
          'Otherwise (used only for testing the SDK), this is '
          'the name of a local directory. '
          'If the string "default", a standard SDK location is used. '
          'If empty, no SDK is copied.'))

# Command line options controlling the worker pool configuration.

# TODO(silviuc): Update description when autoscaling options are in.
add_option(
    '--num_workers', dest='num_workers', type=int,
    default=None,
    help=('Number of workers to use when executing the Dataflow job. If not '
          'set, the Dataflow service will use a reasonable default.'))

add_option(
    '--machine_type', dest='machine_type',
    default=None,
    help=('Machine type to create Dataflow worker VMs as. '
          'See https://cloud.google.com/compute/docs/machine-types for a list '
          'of valid options. If not set, the Dataflow service will choose a '
          'reasonable default.'))

add_option(
    '--disk_size_gb', dest='disk_size_gb', type=int,
    default=None,
    help=('Remote worker disk size, in gigabytes, or 0 to use the default '
          'size. If not set, the Dataflow service will use a reasonable '
          'default.'))

add_option(
    '--disk_type', dest='disk_type',
    default=None,
    help=('Specifies what type of persistent disk should be used.'))

add_option(
    '--disk_source_image', dest='disk_source_image',
    default=None,
    help=('Disk source image to use by VMs for jobs. '
          'See https://developers.google.com/compute/docs/images for '
          'further details. If not set, the Dataflow service '
          'will use a reasonable default.'))

add_option(
    '--zone', dest='zone',
    default=None,
    help=('GCE availability zone for launching workers. '
          'Default is up to the Dataflow service.'))

add_option(
    '--network', dest='network',
    default=None,
    help=('GCE network for launching workers. Default is up to the Dataflow '
          'service.'))

add_option(
    '--teardown_policy', dest='teardown_policy',
    choices=['TEARDOWN_ALWAYS', 'TEARDOWN_NEVER', 'TEARDOWN_ON_SUCCESS'],
    default=None,
    help=('The teardown policy for the VMs. By default this is left unset '
          'and the service sets the default policy.'))

# TODO(silviuc): Add autoscaling related options:
# --autoscaling_algorithm, --max_num_workers.

# TODO(silviuc): Add --files_to_stage option.
# This could potentially replace the --requirements_file and --setup_file.

# TODO(silviuc): Non-standard options. Keep them? If yes, add help too!
# Remote execution must check that this option is not None.
add_option('--no_auth', dest='no_auth', type=bool, default=False)
