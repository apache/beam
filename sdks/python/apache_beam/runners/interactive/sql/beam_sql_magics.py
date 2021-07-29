import logging

from IPython.core.magic import Magics
from IPython.core.magic import cell_magic
from IPython.core.magic import line_cell_magic
from IPython.core.magic import line_magic
from IPython.core.magic import magics_class

import apache_beam as beam
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as inst
from apache_beam.runners.interactive.utils import obfuscate
from apache_beam.runners.interactive.utils import progress_indicated
from apache_beam.transforms.sql import SqlTransform

_LOGGER = logging.getLogger(__name__)

_EXAMPLE_USAGE = """Example Usage:
    %%%%beam_sql source (A pcoll's name)

    SELECT age FROM PCOLLECTION
    WHERE age>15
    GROUP BY age
"""

_INTERACTIVE_LOG_STYLE = """
  <style>
    div.alert {
      white-space: pre-line;
    }
  </style>
"""


@magics_class
class BeamSqlMagics(Magics):
  def __init__(self, shell):
    super(BeamSqlMagics, self).__init__(shell)

  @cell_magic
  def beam_sql(self, line, cell):
    if not line:
      self.on_error('Please supply the source of the sql.')
      return
    if not cell or cell.isspace():
      self.on_error('Please supply the sql to be executed.')
      return
    source, pcoll_names = self.find_pcoll(line)
    if not source:
      self.on_error(
          'Please supply a valid source such as a variable name of a PCollection. '
          'Given invalid value was: %s. Available values are: %s',
          line,
          pcoll_names)
      return
    _LOGGER.info("The source of the sql is %s: %s", line, source)
    ib.show(source)

    @progress_indicated
    def run_sql_from_pcoll():
      execution_id = obfuscate(line, cell, source)[:12]
      cache_manager = ie.current_env().get_cache_manager(source.pipeline)
      instrumentation = inst.build_pipeline_instrument(source.pipeline)
      sql_pipeline = beam.Pipeline()

      sql_source = self.pcoll_from_cache(
          sql_pipeline,
          source,
          cache_manager,
          instrumentation.cache_key(source))

      ib.show_graph(sql_pipeline.to_runner_api())
      #return sql_source
      output_pcoll = sql_source | SqlTransform(cell)

      #sql_pipeline.run()
      output_pcoll_name = 'sql_output_' + execution_id
      ib.watch({output_pcoll_name: output_pcoll})
      _LOGGER.info(
          "The output PCollection is %s: %s", output_pcoll_name, output_pcoll)
      return output_pcoll

    return run_sql_from_pcoll()

  def on_error(self, error_msg, *args):
    from IPython.core.display import HTML
    from IPython.core.display import display
    display(HTML(_INTERACTIVE_LOG_STYLE))
    _LOGGER.error(error_msg, *args)
    _LOGGER.info(_EXAMPLE_USAGE)

  def find_pcoll(self, name):
    inspectables = ie.current_env().inspector.inspectables
    pcoll_names = []
    for _, inspectable in inspectables.items():
      metadata = inspectable['metadata']
      if metadata['type'] == 'pcollection':
        pcoll_names.append(metadata['name'])
        if metadata['name'] == name:
          return inspectable['value'], None
    return None, pcoll_names

  def pcoll_from_cache(self, query_pipeline, pcoll, cache_manager, key):
    class Unreify(beam.DoFn):
      def process(self, e):
        yield e.windowed_value

    coder = cache_manager.load_pcoder(key)
    return (
        query_pipeline
        | '{}{}'.format('QuerySource', key) >> cache.ReadCache(
            cache_manager, key)
        | beam.Map(rebuildRow).with_output_types(pcoll.element_type))
    #| '{}{}'.format('Decode', key) >> beam.Map(lambda x: coder.decode(x))


#      | '{}{}unreify'.format('QuerySource', key) >> beam.ParDo(Unreify()))

  def write_to_cache(self, query_pipeline, pcoll, cache_manager, key):
    pass


def rebuildRow(x):
  fields = type(x)._fields
  kwargs = {}
  for field in fields:
    kwargs[field] = getattr(x, field)
  return beam.Row(**kwargs)


def load_ipython_extension(ipython):
  ipython.register_magics(BeamSqlMagics)
