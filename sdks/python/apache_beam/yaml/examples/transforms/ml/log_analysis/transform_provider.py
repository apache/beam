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

import pickle

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.anomaly.detectors.pyod_adapter import PyODFactory
from apache_beam.ml.anomaly.transforms import AnomalyDetection


class MyTransform(beam.PTransform):
  def __init__(self, model_artifact_path):
    self.model_artifact_path = model_artifact_path
    # self.file = FileSystems.open(model_artifact_path, 'r')
    # self.model = pickle.load(file)
    # self.model = PyODFactory.create_detector(
    #     self.model_artifact_path,
    #     model_id="knn",
    # )

  def expand(self, pcoll):
    # no-op
    return (
        pcoll
        # | beam.Map(print)
        # | beam.Map(lambda x: x.embedding)
        # | AnomalyDetection(detector=self.model)
        # | beam.Map(lambda x: beam.Row(
        #     example=x.example,
        #     predictions=[pred.__dict__ for pred in x.predictions]))
    )
