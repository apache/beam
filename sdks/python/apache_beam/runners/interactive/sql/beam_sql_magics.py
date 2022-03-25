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

"""Module of beam_sql cell magic that executes a Beam SQL.

Only works within an IPython kernel.
"""

import argparse
import importlib
import keyword
import logging
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import apache_beam as beam
from apache_beam.pvalue import PValue
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.background_caching_job import has_source_to_cache
from apache_beam.runners.interactive.caching.cacheable import CacheKey
from apache_beam.runners.interactive.caching.reify import reify_to_cache
from apache_beam.runners.interactive.caching.reify import unreify_from_cache
from apache_beam.runners.interactive.display.pcoll_visualization import visualize_computed_pcoll
from apache_beam.runners.interactive.sql.sql_chain import SqlChain
from apache_beam.runners.interactive.sql.sql_chain import SqlNode
from apache_beam.runners.interactive.sql.utils import DataflowOptionsForm
from apache_beam.runners.interactive.sql.utils import find_pcolls
from apache_beam.runners.interactive.sql.utils import pformat_namedtuple
from apache_beam.runners.interactive.sql.utils import register_coder_for_schema
from apache_beam.runners.interactive.sql.utils import replace_single_pcoll_token
from apache_beam.runners.interactive.utils import create_var_in_main
from apache_beam.runners.interactive.utils import obfuscate
from apache_beam.runners.interactive.utils import pcoll_by_name
from apache_beam.runners.interactive.utils import progress_indicated
from apache_beam.testing import test_stream
from apache_beam.testing.test_stream_service import TestStreamServiceController
from apache_beam.transforms.sql import SqlTransform
from apache_beam.typehints.native_type_compatibility import match_is_named_tuple
from IPython.core.magic import Magics
from IPython.core.magic import line_cell_magic
from IPython.core.magic import magics_class

_LOGGER = logging.getLogger(__name__)

_EXAMPLE_USAGE = """beam_sql magic to execute Beam SQL in notebooks
---------------------------------------------------------
%%beam_sql [-o OUTPUT_NAME] [-v] [-r RUNNER] query
---------------------------------------------------------
Or
---------------------------------------------------------
%%%%beam_sql [-o OUTPUT_NAME] [-v] [-r RUNNER] query-line#1
query-line#2
...
query-line#N
---------------------------------------------------------
"""

_NOT_SUPPORTED_MSG = """The query was valid and successfully applied.
    But beam_sql failed to execute the query: %s

    Runner used by beam_sql was %s.
    Some Beam features might have not been supported by the Python SDK and runner combination.
    Please check the runner output for more details about the failed items.

    In the meantime, you may check:
    https://beam.apache.org/documentation/runners/capability-matrix/
    to choose a runner other than the InteractiveRunner and explicitly apply SqlTransform
    to build Beam pipelines in a non-interactive manner.
"""

_SUPPORTED_RUNNERS = ['DirectRunner', 'DataflowRunner']


class BeamSqlParser:
  """A parser to parse beam_sql inputs."""
  def __init__(self):
    self._parser = argparse.ArgumentParser(usage=_EXAMPLE_USAGE)
    self._parser.add_argument(
        '-o',
        '--output-name',
        dest='output_name',
        help=(
            'The output variable name of the magic, usually a PCollection. '
            'Auto-generated if omitted.'))
    self._parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Display more details about the magic execution.')
    self._parser.add_argument(
        '-r',
        '--runner',
        dest='runner',
        help=(
            'The runner to run the query. Supported runners are %s. If not '
            'provided, DirectRunner is used and results can be inspected '
            'locally.' % _SUPPORTED_RUNNERS))
    self._parser.add_argument(
        'query',
        type=str,
        nargs='*',
        help=(
            'The Beam SQL query to execute. '
            'Syntax: https://beam.apache.org/documentation/dsls/sql/calcite/'
            'query-syntax/. '
            'Please make sure that there is no conflict between your variable '
            'names and the SQL keywords, such as "SELECT", "FROM", "WHERE" and '
            'etc.'))

  def parse(self, args: List[str]) -> Optional[argparse.Namespace]:
    """Parses a list of string inputs.

    The parsed namespace contains these attributes:
      output_name: Optional[str], the output variable name.
      verbose: bool, whether to display more details of the magic execution.
      query: Optional[List[str]], the beam SQL query to execute.

    Returns:
      The parsed args or None if fail to parse.
    """
    try:
      return self._parser.parse_args(args)
    except KeyboardInterrupt:
      raise
    except:  # pylint: disable=bare-except
      # -h or --help results in SystemExit 0. Do not raise.
      return None

  def print_help(self) -> None:
    self._parser.print_help()


def on_error(error_msg, *args):
  """Logs the error and the usage example."""
  _LOGGER.error(error_msg, *args)
  BeamSqlParser().print_help()


@magics_class
class BeamSqlMagics(Magics):
  def __init__(self, shell):
    super().__init__(shell)
    # Eagerly initializes the environment.
    _ = ie.current_env()
    self._parser = BeamSqlParser()

  @line_cell_magic
  def beam_sql(self, line: str, cell: Optional[str] = None) -> Optional[PValue]:
    """The beam_sql line/cell magic that executes a Beam SQL.

    Args:
      line: the string on the same line after the beam_sql magic.
      cell: everything else in the same notebook cell as a string. If None,
        beam_sql is used as line magic. Otherwise, cell magic.

    Returns None if running into an error or waiting for user input (running on
    a selected runner remotely), otherwise a PValue as if a SqlTransform is
    applied.
    """
    input_str = line
    if cell:
      input_str += ' ' + cell
    parsed = self._parser.parse(input_str.strip().split())
    if not parsed:
      # Failed to parse inputs, let the parser handle the exit.
      return
    output_name = parsed.output_name
    verbose = parsed.verbose
    query = parsed.query
    runner = parsed.runner

    if output_name and not output_name.isidentifier() or keyword.iskeyword(
        output_name):
      on_error(
          'The output_name "%s" is not a valid identifier. Please supply a '
          'valid identifier that is not a Python keyword.',
          line)
      return
    if not query:
      on_error('Please supply the SQL query to be executed.')
      return
    if runner and runner not in _SUPPORTED_RUNNERS:
      on_error(
          'Runner "%s" is not supported. Supported runners are %s.',
          runner,
          _SUPPORTED_RUNNERS)
    query = ' '.join(query)

    found = find_pcolls(query, pcoll_by_name(), verbose=verbose)
    schemas = set()
    main_session = importlib.import_module('__main__')
    for _, pcoll in found.items():
      if not match_is_named_tuple(pcoll.element_type):
        on_error(
            'PCollection %s of type %s is not a NamedTuple. See '
            'https://beam.apache.org/documentation/programming-guide/#schemas '
            'for more details.',
            pcoll,
            pcoll.element_type)
        return
      register_coder_for_schema(pcoll.element_type, verbose=verbose)
      # Only care about schemas defined by the user in the main module.
      if hasattr(main_session, pcoll.element_type.__name__):
        schemas.add(pcoll.element_type)

    if runner in ('DirectRunner', None):
      collect_data_for_local_run(query, found)
      output_name, output, chain = apply_sql(query, output_name, found)
      chain.current.schemas = schemas
      cache_output(output_name, output)
      return output

    output_name, current_node, chain = apply_sql(
        query, output_name, found, False)
    current_node.schemas = schemas
    # TODO(BEAM-10708): Move the options setup and result handling to a
    # separate module when more runners are supported.
    if runner == 'DataflowRunner':
      _ = chain.to_pipeline()
      _ = DataflowOptionsForm(
          output_name, pcoll_by_name()[output_name],
          verbose).display_for_input()
      return None
    else:
      raise ValueError('Unsupported runner %s.', runner)


@progress_indicated
def collect_data_for_local_run(query: str, found: Dict[str, beam.PCollection]):
  from apache_beam.runners.interactive import interactive_beam as ib
  for name, pcoll in found.items():
    try:
      _ = ib.collect(pcoll)
    except (KeyboardInterrupt, SystemExit):
      raise
    except:
      _LOGGER.error(
          'Cannot collect data for PCollection %s. Please make sure the '
          'PCollections queried in the sql "%s" are all from a single '
          'pipeline using an InteractiveRunner. Make sure there is no '
          'ambiguity, for example, same named PCollections from multiple '
          'pipelines or notebook re-executions.',
          name,
          query)
      raise


@progress_indicated
def apply_sql(
    query: str,
    output_name: Optional[str],
    found: Dict[str, beam.PCollection],
    run: bool = True) -> Tuple[str, Union[PValue, SqlNode], SqlChain]:
  """Applies a SqlTransform with the given sql and queried PCollections.

  Args:
    query: The SQL query executed in the magic.
    output_name: (optional) The output variable name in __main__ module.
    found: The PCollections with variable names found to be used in the query.
    run: Whether to prepare the SQL pipeline for a local run or not.

  Returns:
    A tuple of values. First str value is the output variable name in
    __main__ module, auto-generated if not provided. Second value: if run,
    it's a PValue; otherwise, a SqlNode tracks the SQL without applying it or
    executing it. Third value: SqlChain is a chain of SqlNodes that have been
    applied.
  """
  output_name = _generate_output_name(output_name, query, found)
  query, sql_source, chain = _build_query_components(
      query, found, output_name, run)
  if run:
    try:
      output = sql_source | SqlTransform(query)
      # Declare a variable with the output_name and output value in the
      # __main__ module so that the user can use the output smoothly.
      output_name, output = create_var_in_main(output_name, output)
      _LOGGER.info(
          "The output PCollection variable is %s with element_type %s",
          output_name,
          pformat_namedtuple(output.element_type))
      return output_name, output, chain
    except (KeyboardInterrupt, SystemExit):
      raise
    except Exception as e:
      on_error('Error when applying the Beam SQL: %s', e)
  else:
    return output_name, chain.current, chain


def pcolls_from_streaming_cache(
    user_pipeline: beam.Pipeline,
    query_pipeline: beam.Pipeline,
    name_to_pcoll: Dict[str, beam.PCollection]) -> Dict[str, beam.PCollection]:
  """Reads PCollection cache through the TestStream.

  Args:
    user_pipeline: The beam.Pipeline object defined by the user in the
        notebook.
    query_pipeline: The beam.Pipeline object built by the magic to execute the
        SQL query.
    name_to_pcoll: PCollections with variable names used in the SQL query.

  Returns:
    A Dict[str, beam.PCollection], where each PCollection is tagged with
    their PCollection variable names, read from the cache.

  When the user_pipeline has unbounded sources, we force all cache reads to go
  through the TestStream even if they are bounded sources.
  """
  def exception_handler(e):
    _LOGGER.error(str(e))
    return True

  cache_manager = ie.current_env().get_cache_manager(
      user_pipeline, create_if_absent=True)
  test_stream_service = ie.current_env().get_test_stream_service_controller(
      user_pipeline)
  if not test_stream_service:
    test_stream_service = TestStreamServiceController(
        cache_manager, exception_handler=exception_handler)
    test_stream_service.start()
    ie.current_env().set_test_stream_service_controller(
        user_pipeline, test_stream_service)

  tag_to_name = {}
  for name, pcoll in name_to_pcoll.items():
    key = CacheKey.from_pcoll(name, pcoll).to_str()
    tag_to_name[key] = name
  output_pcolls = query_pipeline | test_stream.TestStream(
      output_tags=set(tag_to_name.keys()),
      coder=cache_manager._default_pcoder,
      endpoint=test_stream_service.endpoint)
  sql_source = {}
  for tag, output in output_pcolls.items():
    name = tag_to_name[tag]
    # Must mark the element_type to avoid introducing pickled Python coder
    # to the Java expansion service.
    output.element_type = name_to_pcoll[name].element_type
    sql_source[name] = output
  return sql_source


def _generate_output_name(
    output_name: Optional[str], query: str,
    found: Dict[str, beam.PCollection]) -> str:
  """Generates a unique output name if None is provided.

  Otherwise, returns the given output name directly.
  The generated output name is sql_output_{uuid} where uuid is an obfuscated
  value from the query and PCollections found to be used in the query.
  """
  if not output_name:
    execution_id = obfuscate(query, found)[:12]
    output_name = 'sql_output_' + execution_id
  return output_name


def _build_query_components(
    query: str,
    found: Dict[str, beam.PCollection],
    output_name: str,
    run: bool = True
) -> Tuple[str,
           Union[Dict[str, beam.PCollection], beam.PCollection, beam.Pipeline],
           SqlChain]:
  """Builds necessary components needed to apply the SqlTransform.

  Args:
    query: The SQL query to be executed by the magic.
    found: The PCollections with variable names found to be used by the query.
    output_name: The output variable name in __main__ module.
    run: Whether to prepare components for a local run or not.

  Returns:
    The processed query to be executed by the magic; a source to apply the
    SqlTransform to: a dictionary of tagged PCollections, or a single
    PCollection, or the pipeline to execute the query; the chain of applied
    beam_sql magics this one belongs to.
  """
  if found:
    user_pipeline = ie.current_env().user_pipeline(
        next(iter(found.values())).pipeline)
    sql_pipeline = beam.Pipeline(options=user_pipeline._options)
    ie.current_env().add_derived_pipeline(user_pipeline, sql_pipeline)
    sql_source = {}
    if run:
      if has_source_to_cache(user_pipeline):
        sql_source = pcolls_from_streaming_cache(
            user_pipeline, sql_pipeline, found)
      else:
        cache_manager = ie.current_env().get_cache_manager(
            user_pipeline, create_if_absent=True)
        for pcoll_name, pcoll in found.items():
          cache_key = CacheKey.from_pcoll(pcoll_name, pcoll).to_str()
          sql_source[pcoll_name] = unreify_from_cache(
              pipeline=sql_pipeline,
              cache_key=cache_key,
              cache_manager=cache_manager,
              element_type=pcoll.element_type)
    else:
      sql_source = found
    if len(sql_source) == 1:
      query = replace_single_pcoll_token(query, next(iter(sql_source.keys())))
      sql_source = next(iter(sql_source.values()))

    node = SqlNode(
        output_name=output_name, source=set(found.keys()), query=query)
    chain = ie.current_env().get_sql_chain(
        user_pipeline, set_user_pipeline=True).append(node)
  else:  # does not query any existing PCollection
    sql_source = beam.Pipeline()
    ie.current_env().add_user_pipeline(sql_source)

    # The node should be the root node of the chain created below.
    node = SqlNode(output_name=output_name, source=sql_source, query=query)
    chain = ie.current_env().get_sql_chain(sql_source).append(node)
  return query, sql_source, chain


@progress_indicated
def cache_output(output_name: str, output: PValue) -> None:
  user_pipeline = ie.current_env().user_pipeline(output.pipeline)
  if user_pipeline:
    cache_manager = ie.current_env().get_cache_manager(
        user_pipeline, create_if_absent=True)
  else:
    _LOGGER.warning(
        'Something is wrong with %s. Cannot introspect its data.', output)
    return
  key = CacheKey.from_pcoll(output_name, output).to_str()
  _ = reify_to_cache(pcoll=output, cache_key=key, cache_manager=cache_manager)
  try:
    output.pipeline.run().wait_until_finish()
  except (KeyboardInterrupt, SystemExit):
    raise
  except Exception as e:
    _LOGGER.warning(_NOT_SUPPORTED_MSG, e, output.pipeline.runner)
    return
  ie.current_env().mark_pcollection_computed([output])
  visualize_computed_pcoll(
      output_name, output, max_n=float('inf'), max_duration_secs=float('inf'))


def load_ipython_extension(ipython):
  """Marks this module as an IPython extension.

  To load this magic in an IPython environment, execute:
  %load_ext apache_beam.runners.interactive.sql.beam_sql_magics.
  """
  ipython.register_magics(BeamSqlMagics)
