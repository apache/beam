"""Preprocessing example with TFX with the LocalDagRunner and
either the beam DirectRunner or DataflowRunner"""
import os

from tfx import v1 as tfx

PROJECT_ID = "<project id>"
PROJECT_REGION = "<project region>"
PIPELINE_NAME = "<pipeline_name>"
PIPELINE_ROOT = "<path to pipeline root>"
INPUT_DATA_PATH = "<path to csv input file>"
MODULE_FILE = "<path to module file containing preprocessing_fn and run_fn>"
BEAM_RUNNER = "<Beam runner: DataflowRunner or DirectRunner>"
METADATA_FILE = "<path to store a metadata file as a mock metadata database>"

BEAM_PIPELINE_ARGS_BY_RUNNER = {
    'DirectRunner': [],
    'DataflowRunner': [
        '--runner=DataflowRunner',
        '--project=' + PROJECT_ID,
        '--temp_location=' + os.path.join(PIPELINE_ROOT, 'tmp'),
        '--region=' + PROJECT_REGION,
    ]
}


# [START tfx_pipeline]
def create_pipeline():
  example_gen = tfx.components.CsvExampleGen(input_base=INPUT_DATA_PATH)

  # Computes statistics over data for visualization and example validation.
  statistics_gen = tfx.components.StatisticsGen(
      examples=example_gen.outputs['examples'])

  schema_gen = tfx.components.SchemaGen(
      statistics=statistics_gen.outputs['statistics'], infer_feature_shape=True)

  transform = tfx.components.Transform(
      examples=example_gen.outputs['examples'],
      schema=schema_gen.outputs['schema'],
      module_file=MODULE_FILE)

  trainer = tfx.components.Trainer(
      module_file="coco_captions_utils.py",
      examples=transform.outputs['transformed_examples'],
      transform_graph=transform.outputs['transform_graph'])

  components = [example_gen, statistics_gen, schema_gen, transform, trainer]

  return tfx.dsl.Pipeline(
      pipeline_name=PIPELINE_NAME,
      pipeline_root=PIPELINE_ROOT,
      components=components,
      enable_cache=True,
      metadata_connection_config=tfx.orchestration.metadata.
      sqlite_metadata_connection_config(METADATA_FILE),
      beam_pipeline_args=BEAM_PIPELINE_ARGS_BY_RUNNER[BEAM_RUNNER])


# [END tfx_pipeline]

if __name__ == "__main__":
  # [START tfx_execute_pipeline]
  tfx.orchestration.LocalDagRunner().run(create_pipeline())
  # [END tfx_execute_pipeline]
