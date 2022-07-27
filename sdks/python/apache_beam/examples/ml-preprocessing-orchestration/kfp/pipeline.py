import logging
import argparse

from kfp import components as comp
from kfp.v2 import dsl
from kfp.v2.compiler import Compiler


# load the kfp components from their yaml files
DataIngestOp = comp.load_component(
    'components/ingestion/component.yaml'
)
DataPreprocessingOp = comp.load_component(
    'components/preprocessing/component.yaml'
)

TrainModelOp = comp.load_component(
    'components/train/component.yaml'
)

PIPELINE_ROOT = "<pipelin-root-path>"
BASE_ARTIFACT_PATH = "<base-path-to-store-pipeline-artifacts"

@dsl.pipeline(
    pipeline_root=PIPELINE_ROOT,
    name="beam-preprocessing-kfp-example",
    description="Pipeline to show an apache beam preprocessing example in KFP"
)
def pipeline(base_artifact_path: str = BASE_ARTIFACT_PATH):

    ingest_data_task = DataIngestOp(
        base_artifact_path=base_artifact_path
    )
    data_preprocessing_task = DataPreprocessingOp(
        ingested_dataset_path=ingest_data_task.outputs["ingested_dataset_path"],
        base_artifact_path=base_artifact_path
    )

    train_model_task = TrainModelOp(
        preprocessed_dataset_path=data_preprocessing_task.outputs["preprocessed_dataset_path"],
        base_artifact_path=base_artifact_path
    )

if __name__ == "__main__":
    Compiler().compile(
        pipeline_func=pipeline,
        package_path="pipeline.json"
    )
