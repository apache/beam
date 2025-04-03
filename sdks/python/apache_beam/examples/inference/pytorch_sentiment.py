# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import logging
from collections.abc import Iterable

import apache_beam as beam
import torch
import torch.nn.functional as F
from apache_beam.ml.inference.base import KeyedModelHandler, PredictionResult, RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.runners.runner import PipelineResult
from transformers import DistilBertTokenizerFast, DistilBertForSequenceClassification


class SentimentPostProcessor(beam.DoFn):
    def __init__(self, tokenizer: DistilBertTokenizerFast):
        self.tokenizer = tokenizer

    def process(self, element: tuple[str, PredictionResult]) -> Iterable[str]:
        text, prediction_result = element
        logits = prediction_result.inference['logits']
        probs = F.softmax(logits, dim=-1)
        predicted_class = torch.argmax(probs).item()
        confidence = probs[0][predicted_class].item()
        sentiment = 'POSITIVE' if predicted_class == 1 else 'NEGATIVE'
        yield f"{text};{sentiment};{confidence:.4f}"


def tokenize_text(text: str, tokenizer: DistilBertTokenizerFast) -> tuple[str, dict[str, torch.Tensor]]:
    tokenized = tokenizer(text, padding='max_length', truncation=True, max_length=128, return_tensors="pt")
    return text, {k: torch.squeeze(v) for k, v in tokenized.items()}


def parse_known_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True, help='Path to output file')
    parser.add_argument('--model_path', default='distilbert-base-uncased-finetuned-sst-2-english',
                        help='Hugging Face model name or local path')
    return parser.parse_known_args(argv)


def run(argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
    known_args, pipeline_args = parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    model_handler = PytorchModelHandlerKeyedTensor(
        model_class=DistilBertForSequenceClassification,
        model_params={'num_labels': 2},
        state_dict_path=None,
        model_uri=known_args.model_path
    )

    tokenizer = DistilBertTokenizerFast.from_pretrained(known_args.model_path)

    pipeline = test_pipeline or beam.Pipeline(options=pipeline_options)

    texts = [
        "I love using Apache Beam for data pipelines!",
        "This model doesn't work at all.",
        "The weather is okay, not great.",
        "What a fantastic movie, 10 out of 10!",
        "Terrible customer service experience."
    ]

    (
            pipeline
            | 'CreateInput' >> beam.Create(texts)
            | 'Tokenize' >> beam.Map(lambda text: tokenize_text(text, tokenizer))
            | 'RunInference' >> RunInference(KeyedModelHandler(model_handler))
            | 'PostProcess' >> beam.ParDo(SentimentPostProcessor(tokenizer))
            | 'WriteOutput' >> beam.io.WriteToText(known_args.output, shard_name_template='')
    )

    result = pipeline.run()
    result.wait_until_finish()
    return result


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
