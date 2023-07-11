# coding=utf-8
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

# pytype: skip-file
# pylint: disable=reimported
# pylint:disable=line-too-long

# beam-playground:
#   name: RunInferenceSklearnKeyed
#   description: Demonstration of RunInference transform usage with Sklearn keyed model handler.
#   multifile: false
#   default_example: false
#   context_line: 46
#   categories:
#     - Core Transforms
#   complexity: BASIC
#   tags:
#     - transforms
#     - inference
#     - sklearn


def sklearn_keyed_model_handler(test=None):
  # [START sklearn_keyed_model_handler]
  import apache_beam as beam
  from apache_beam.ml.inference.base import KeyedModelHandler
  from apache_beam.ml.inference.base import RunInference
  from apache_beam.ml.inference.sklearn_inference import ModelFileType
  from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy

  sklearn_model_filename = 'gs://apache-beam-samples/run_inference/five_times_table_sklearn.pkl'  # pylint: disable=line-too-long
  sklearn_model_handler = KeyedModelHandler(
      SklearnModelHandlerNumpy(
          model_uri=sklearn_model_filename,
          model_file_type=ModelFileType.PICKLE))

  keyed_data = [("first_question", 105.00), ("second_question", 108.00),
                ("third_question", 1000.00), ("fourth_question", 1013.00)]

  with beam.Pipeline() as p:
    predictions = (
        p
        | "ReadInputs" >> beam.Create(keyed_data)
        | "ConvertDataToList" >> beam.Map(lambda x: (x[0], [x[1]]))
        | "RunInferenceSklearn" >>
        RunInference(model_handler=sklearn_model_handler)
        | beam.Map(print))
    # [END sklearn_keyed_model_handler]
    if test:
      test(predictions)


if __name__ == '__main__':
  sklearn_keyed_model_handler()
