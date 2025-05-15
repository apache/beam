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

"""Module of utilities for SQL magics.

For internal use only; no backward-compatibility guarantees.
"""

# pytype: skip-file

import logging
import os
import tempfile
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import NamedTuple
from typing import Optional
from typing import Type
from typing import Union

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.runners.interactive.utils import create_var_in_main
from apache_beam.runners.interactive.utils import progress_indicated
from apache_beam.runners.runner import create_runner
from apache_beam.typehints.native_type_compatibility import match_is_named_tuple
from apache_beam.utils.interactive_utils import is_in_ipython

_LOGGER = logging.getLogger(__name__)


def register_coder_for_schema(
    schema: NamedTuple, verbose: bool = False) -> None:
  """Registers a RowCoder for the given schema if hasn't.

  Notifies the user of what code has been implicitly executed.
  """
  assert match_is_named_tuple(schema), (
      'Schema %s is not a typing.NamedTuple.' % schema)
  coder = beam.coders.registry.get_coder(schema)
  if not isinstance(coder, beam.coders.RowCoder):
    if verbose:
      _LOGGER.warning(
          'Schema %s has not been registered to use a RowCoder. '
          'Automatically registering it by running: '
          'beam.coders.registry.register_coder(%s, '
          'beam.coders.RowCoder)',
          schema.__name__,
          schema.__name__)
    beam.coders.registry.register_coder(schema, beam.coders.RowCoder)


def find_pcolls(
    sql: str,
    pcolls: Dict[str, beam.PCollection],
    verbose: bool = False) -> Dict[str, beam.PCollection]:
  """Finds all PCollections used in the given sql query.

  It does a simple word by word match and calls ib.collect for each PCollection
  found.
  """
  found = {}
  for word in sql.split():
    if word in pcolls:
      found[word] = pcolls[word]
  if found:
    if verbose:
      _LOGGER.info('Found PCollections used in the magic: %s.', found)
      _LOGGER.info('Collecting data...')
  return found


def replace_single_pcoll_token(sql: str, pcoll_name: str) -> str:
  """Replaces the pcoll_name used in the sql with 'PCOLLECTION'.

  For sql query using only a single PCollection, the PCollection needs to be
  referred to as 'PCOLLECTION' instead of its variable/tag name.
  """
  words = sql.split()
  token_locations = []
  i = 0
  for word in words:
    if word.lower() == 'from':
      token_locations.append(i + 1)
      i += 2
      continue
    i += 1
  for token_location in token_locations:
    if token_location < len(words) and words[token_location] == pcoll_name:
      words[token_location] = 'PCOLLECTION'
  return ' '.join(words)


def pformat_namedtuple(schema: NamedTuple) -> str:
  return '{}({})'.format(
      schema.__name__,
      ', '.join([
          '{}: {}'.format(k, repr(v))
          for k, v in schema.__annotations__.items()
      ]))


def pformat_dict(raw_input: Dict[str, Any]) -> str:
  return '{{\n{}\n}}'.format(
      ',\n'.join(['{}: {}'.format(k, v) for k, v in raw_input.items()]))


@dataclass
class OptionsEntry:
  """An entry of PipelineOptions that can be visualized through ipywidgets to
  take inputs in IPython notebooks interactively.

  Attributes:
    label: The value of the Label widget.
    help: The help message of the entry, usually the same to the help in
      PipelineOptions.
    cls: The PipelineOptions class/subclass the options belong to.
    arg_builder: Builds the argument/option. If it's a str, this entry
      assigns the input ipywidget's value directly to the argument. If it's a
      Dict, use the corresponding Callable to assign the input value to each
      argument. If Callable is None, fallback to assign the input value
      directly. This allows building multiple similar PipelineOptions
      arguments from a single input, such as staging_location and
      temp_location in GoogleCloudOptions.
    default: The default value of the entry, None if absent.
  """
  label: str
  help: str
  cls: Type[PipelineOptions]
  arg_builder: Union[str, Dict[str, Optional[Callable]]]
  default: Optional[str] = None

  def __post_init__(self):
    # The attribute holds an ipywidget, currently only supports Text.
    # The str value can be accessed by self.input.value.
    self.input = None


class OptionsForm:
  """A form visualized to take inputs from users in IPython Notebooks and
  generate PipelineOptions to run pipelines.
  """
  def __init__(self):
    # The current Python SDK incorrectly parses unparsable pipeline options
    # Here we ignore all flags for the interactive beam_sql magic
    # since the beam_sql magic does not use flags
    self.options = PipelineOptions(flags={})
    self.entries = []

  def add(self, entry: OptionsEntry) -> 'OptionsForm':
    """Adds an OptionsEntry to the form.
    """
    self.entries.append(entry)
    return self

  def to_options(self) -> PipelineOptions:
    """Builds the PipelineOptions based on user inputs.

    Can only be invoked after display_for_input.
    """
    for entry in self.entries:
      assert entry.input, (
          'to_options invoked before display_for_input. '
          'Wrong usage.')
      view = self.options.view_as(entry.cls)
      if isinstance(entry.arg_builder, str):
        setattr(view, entry.arg_builder, entry.input.value)
      else:
        for arg, builder in entry.arg_builder.items():
          if builder:
            setattr(view, arg, builder(entry.input.value))
          else:
            setattr(view, arg, entry.input.value)
    self.additional_options()
    return self.options

  def additional_options(self):
    """Alters the self.options with additional config."""
    pass

  def display_for_input(self) -> 'OptionsForm':
    """Displays the widgets to take user inputs."""
    from IPython.display import display
    from ipywidgets import GridBox
    from ipywidgets import Label
    from ipywidgets import Layout
    from ipywidgets import Text
    widgets = []
    for entry in self.entries:
      text_label = Label(value=entry.label)
      text_input = entry.input if entry.input else Text(
          value=entry.default if entry.default else '')
      text_help = Label(value=entry.help)
      entry.input = text_input
      widgets.append(text_label)
      widgets.append(text_input)
      widgets.append(text_help)
    grid = GridBox(widgets, layout=Layout(grid_template_columns='1fr 2fr 6fr'))
    display(grid)
    self.display_actions()
    return self

  def display_actions(self):
    """Displays actionable widgets to utilize the options, run pipelines and
    etc."""
    pass


class DataflowOptionsForm(OptionsForm):
  """A form to take inputs from users in IPython Notebooks to build
  PipelineOptions to run pipelines on Dataflow.

  Only contains minimum fields needed.
  """
  @staticmethod
  def _build_default_project() -> str:
    """Builds a default project id."""
    try:
      # pylint: disable=c-extension-no-member
      import google.auth
      return google.auth.default()[1]
    except (KeyboardInterrupt, SystemExit):
      raise
    except Exception as e:
      _LOGGER.warning('There is some issue with your gcloud auth: %s', e)
      return 'your-project-id'

  @staticmethod
  def _build_req_file_from_pkgs(pkgs) -> Optional[str]:
    """Builds a requirements file that contains all additional PYPI packages
    needed."""
    if pkgs:
      deps = pkgs.split(',')
      req_file = os.path.join(
          tempfile.mkdtemp(prefix='beam-sql-dataflow-'), 'req.txt')
      with open(req_file, 'a') as f:
        for dep in deps:
          f.write(dep.strip() + '\n')
      return req_file
    return None

  def __init__(
      self,
      output_name: str,
      output_pcoll: beam.PCollection,
      verbose: bool = False):
    """Inits the OptionsForm for setting up Dataflow jobs."""
    super().__init__()
    self.p = output_pcoll.pipeline
    self.output_name = output_name
    self.output_pcoll = output_pcoll
    self.verbose = verbose
    self.notice_shown = False
    self.add(
        OptionsEntry(
            label='Project Id',
            help='Name of the Cloud project owning the Dataflow job.',
            cls=GoogleCloudOptions,
            arg_builder='project',
            default=DataflowOptionsForm._build_default_project())
    ).add(
        OptionsEntry(
            label='Region',
            help='The Google Compute Engine region for creating Dataflow job.',
            cls=GoogleCloudOptions,
            arg_builder='region',
            default='us-central1')
    ).add(
        OptionsEntry(
            label='GCS Bucket',
            help=(
                'GCS path to stage code packages needed by workers and save '
                'temporary workflow jobs.'),
            cls=GoogleCloudOptions,
            arg_builder={
                'staging_location': lambda x: x + '/staging',
                'temp_location': lambda x: x + '/temp'
            },
            default='gs://YOUR_GCS_BUCKET_HERE')
    ).add(
        OptionsEntry(
            label='Additional Packages',
            help=(
                'PYPI packages installed, comma-separated. If None, leave '
                'this field empty.'),
            cls=SetupOptions,
            arg_builder={
                'requirements_file': lambda x: DataflowOptionsForm.
                _build_req_file_from_pkgs(x)
            },
            default=''))

  def additional_options(self):
    # Use the latest Java SDK by default.
    sdk_overrides = self.options.view_as(
        WorkerOptions).sdk_harness_container_image_overrides
    override = '.*java.*,apache/beam_java11_sdk:latest'
    if sdk_overrides and override not in sdk_overrides:
      sdk_overrides.append(override)
    else:
      self.options.view_as(
          WorkerOptions).sdk_harness_container_image_overrides = [override]

  def display_actions(self):
    from IPython.display import HTML
    from IPython.display import display
    from ipywidgets import Button
    from ipywidgets import GridBox
    from ipywidgets import Layout
    from ipywidgets import Output
    options_output_area = Output()
    run_output_area = Output()
    run_btn = Button(
        description='Run on Dataflow',
        button_style='success',
        tooltip=(
            'Submit to Dataflow for execution with the configured options. The '
            'output PCollection\'s data will be written to the GCS bucket you '
            'configure.'))
    show_options_btn = Button(
        description='Show Options',
        button_style='info',
        tooltip='Show current pipeline options configured.')

    def _run_on_dataflow(btn):
      with run_output_area:
        run_output_area.clear_output()

        @progress_indicated
        def _inner():
          options = self.to_options()
          # Caches the output_pcoll to a GCS bucket.
          try:
            execution_count = 0
            if is_in_ipython():
              from IPython import get_ipython
              execution_count = get_ipython().execution_count
            output_location = '{}/{}'.format(
                options.view_as(GoogleCloudOptions).staging_location,
                self.output_name)
            _ = self.output_pcoll | 'WriteOuput{}_{}ToGCS'.format(
                self.output_name,
                execution_count) >> WriteToText(output_location)
            _LOGGER.info(
                'Data of output PCollection %s will be written to %s',
                self.output_name,
                output_location)
          except (KeyboardInterrupt, SystemExit):
            raise
          except:  # pylint: disable=bare-except
            # The transform has been added before, noop.
            pass
          if self.verbose:
            _LOGGER.info(
                'Running the pipeline on Dataflow with pipeline options %s.',
                pformat_dict(options.display_data()))
          result = create_runner('DataflowRunner').run_pipeline(self.p, options)
          cloud_options = options.view_as(GoogleCloudOptions)
          url = (
              'https://console.cloud.google.com/dataflow/jobs/%s/%s?project=%s'
              % (cloud_options.region, result.job_id(), cloud_options.project))
          display(
              HTML(
                  'Click <a href="%s" target="_new">here</a> for the details '
                  'of your Dataflow job.' % url))
          result_name = 'result_{}'.format(self.output_name)
          create_var_in_main(result_name, result)
          if self.verbose:
            _LOGGER.info(
                'The pipeline result of the run can be accessed from variable '
                '%s. The current status is %s.',
                result_name,
                result)

        try:
          btn.disabled = True
          _inner()
        finally:
          btn.disabled = False

    run_btn.on_click(_run_on_dataflow)

    def _show_options(btn):
      with options_output_area:
        options_output_area.clear_output()
        options = self.to_options()
        options_name = 'options_{}'.format(self.output_name)
        create_var_in_main(options_name, options)
        _LOGGER.info(
            'The pipeline options configured is: %s.',
            pformat_dict(options.display_data()))

    show_options_btn.on_click(_show_options)
    grid = GridBox([run_btn, show_options_btn],
                   layout=Layout(grid_template_columns='repeat(2, 200px)'))
    display(grid)

    # Implicitly initializes the options variable before 1st time showing
    # options.
    options_name_inited, _ = create_var_in_main('options_{}'.format(
        self.output_name), self.to_options())
    if not self.notice_shown:
      _LOGGER.info(
          'The pipeline options can be configured through variable %s. You '
          'may also add additional options or sink transforms such as write '
          'to BigQuery in other notebook cells. Come back to click "Run on '
          'Dataflow" button once you complete additional configurations. '
          'Optionally, you can chain more beam_sql magics with DataflowRunner '
          'and click "Run on Dataflow" in their outputs.',
          options_name_inited)
      self.notice_shown = True

    display(options_output_area)
    display(run_output_area)
