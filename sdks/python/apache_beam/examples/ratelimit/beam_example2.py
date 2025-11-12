import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from apache_beam.runners.worker.sdk_worker import get_ai_worker_pool_metadata

logging.basicConfig(level=logging.INFO)

class PrintFn(beam.DoFn):
  def process(self, element):
    logging.info(f"Processing element: {element} and worker metadata {get_ai_worker_pool_metadata()}")
    yield element

pipeline_options = PipelineOptions()
pipeline = beam.Pipeline(options=pipeline_options)

# Create a PCollection from a list of elements for this batch job.
data = pipeline | 'Create' >> beam.Create([
  'Hello',
  'World',
  'This',
  'is',
  'a',
  'batch',
  'example',
])

# Apply the custom DoFn with resource hints.
data | 'PrintWithDoFn' >> beam.ParDo(PrintFn())

result = pipeline.run()
result.wait_until_finish()
