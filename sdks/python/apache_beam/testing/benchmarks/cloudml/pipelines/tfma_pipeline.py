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

import tensorflow_model_analysis as tfma
from apache_beam.testing.benchmarks.cloudml.criteo_tft import criteo


def setup_tfma_pipeline(args, input_data):
  """Setup TFMA Dataflow pipeline for the Criteo dataset.

  Args:
    args: Arguments.
    input_data: Input data.
  """

  _ = (
      input_data
      | 'ExtractEvaluateAndWriteResults' >> tfma.ExtractEvaluateAndWriteResults(
          eval_shared_model=tfma.default_eval_shared_model(
              eval_saved_model_path=args.model_dir,
              add_metrics_callbacks=[
                  tfma.post_export_metrics.
                  calibration_plot_and_prediction_histogram(),
                  tfma.post_export_metrics.auc()
              ]),
          slice_spec=[
              tfma.slicer.SingleSliceSpec(
                  columns=[criteo.get_transformed_categorical_column_name(19)]),
              tfma.slicer.SingleSliceSpec(
                  columns=[
                      criteo.get_transformed_categorical_column_name(19),
                      criteo.get_transformed_categorical_column_name(27)
                  ])
          ],
          output_path=args.output))
