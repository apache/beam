import argparse
import sys

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.ml.inference.base import KeyedModelHandler, RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from transformers import AutoConfig

import config as cfg
from pipeline.options import get_pipeline_options
from pipeline.transformations import (
    Decode,
    GetUpdates,
    ModelWrapper,
    NormalizeEmbedding,
    StatefulOnlineClustering,
    tokenize_sentence,
)


def parse_arguments(argv):
    parser = argparse.ArgumentParser(description="online-clustering")

    parser.add_argument(
        "-m",
        "--mode",
        help="Mode to run pipeline in.",
        choices=["local", "cloud"],
        default="local",
    )
    parser.add_argument(
        "-p",
        "--project",
        help="GCP project to run pipeline on.",
        default=cfg.PROJECT_ID,
    )

    args, _ = parser.parse_known_args(args=argv)
    return args


class PytorchNoBatchModelHandler(PytorchModelHandlerKeyedTensor):
    """Wrapper to PytorchModelHandler to limit batch size to 1.
    The tokenized strings generated from BertTokenizer may have different
    lengths, which doesn't work with torch.stack() in current RunInference
    implementation since stack() requires tensors to be the same size.
    Restricting max_batch_size to 1 means there is only 1 example per `batch`
    in the run_inference() call.
    """

    def batch_elements_kwargs(self):
        return {"max_batch_size": 1}


def run():
    args = parse_arguments(sys.argv)
    pipeline_options = get_pipeline_options(
        job_name=cfg.JOB_NAME,
        num_workers=cfg.NUM_WORKERS,
        project=args.project,
        mode=args.mode,
    )

    model_handler = PytorchNoBatchModelHandler(
        state_dict_path=cfg.MODEL_STATE_DICT_PATH,
        model_class=ModelWrapper,
        model_params={"config": AutoConfig.from_pretrained(cfg.MODEL_CONFIG_PATH)},
        device="cpu",
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        docs = (
            pipeline
            | "Read from PubSub"
            >> ReadFromPubSub(subscription=cfg.SUBSCRIPTION_ID, with_attributes=True)
            | "Decode PubSubMessage" >> beam.ParDo(Decode())
        )
        normalized_embedding = (
            docs
            | "Tokenize Text" >> beam.Map(tokenize_sentence)
            | "Get Embedding" >> RunInference(KeyedModelHandler(model_handler))
            | "Normalize Embedding" >> beam.ParDo(NormalizeEmbedding())
        )
        clustering = (
            normalized_embedding
            | "Map doc to key" >> beam.Map(lambda x: (1, x))
            | "StatefulClustering using Birch" >> beam.ParDo(StatefulOnlineClustering())
        )

        updated_clusters = clustering | "Format Update" >> beam.ParDo(GetUpdates())


if __name__ == "__main__":
    run()
