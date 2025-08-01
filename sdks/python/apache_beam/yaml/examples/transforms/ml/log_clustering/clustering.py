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


import apache_beam as beam
import numpy as np
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.embeddings.huggingface import SentenceTransformerEmbeddings
from apache_beam.ml.anomaly.detectors.pyod_adapter import PyODFactory
from apache_beam.ml.anomaly.transforms import AnomalyDetection
from numpy import dtype


class _NormalizeEmbedding(beam.DoFn):
  def __init__(self, column):
    self.column = column

  def process(self, element, *args, **kwargs):
    dict_from_element = element.as_dict()
    embedding = dict_from_element[self.column]
    l2_norm = np.linalg.norm(embedding)
    yield {**dict_from_element, self.column: embedding / l2_norm}


class Normalize(beam.PTransform):
  def __init__(self, column):
    self.column = column

  def expand(self, pcoll):
    return (
        pcoll
        | "Normalize Embedding" >> beam.ParDo(
            _NormalizeEmbedding(self.column))
        | "Convert to Row" >> beam.Map(lambda x: beam.Row(
            id=x['id'],
            log_message=x['log_message'],
            embedding=x['embedding']))
    )


class TextEmbedding(beam.PTransform):
  def __init__(self, model_name, columns, artifact_location):
    self.model_name = model_name
    self.columns = columns
    self.artifact_location = artifact_location

  def expand(self, pcoll):
    embedding_config = SentenceTransformerEmbeddings(
        model_name=self.model_name,
        columns=self.columns,
    )

    return (
        pcoll
        | "MLTransform" >> MLTransform(
            write_artifact_location=self.artifact_location)
            .with_transform(embedding_config)
        | "Convert to Row" >> beam.Map(lambda x: beam.Row(
            id=x.id,
            log_message=x.log_message,
            embedding=x.embedding))
    )


class KNN(beam.PTransform):
  def __init__(self, model_artifact_path):
    self.model_artifact_path = model_artifact_path
    self.model = PyODFactory.create_detector(
        self.model_artifact_path,
        model_id="knn",
    )

  def expand(self, pcoll):
    # return pcoll | "DEBUGGING" >> beam.Map(print)
    return (
        pcoll
        | beam.Map(lambda x: x.embedding)
        | AnomalyDetection(detector=self.model)
        | beam.Map(lambda x: beam.Row(
            example=x.example,
            predictions=[pred.__dict__ for pred in x.predictions]))
        # | "Log" >> beam.LogElements()
    )
