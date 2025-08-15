#
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
#

"""
A script that trains a k-Nearest Neighbors (KNN) model on vector embeddings
data queried from BigQuery.
"""

import argparse
import logging
import pickle

import numpy as np
from google.cloud import bigquery

from pyod.models.knn import KNN


def parse_arguments():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--bq_table',
      required=True,
      help='BigQuery fully qualified table name that contains vector '
      'embeddings for training, '
      'specified as `YOUR_PROJECT.YOUR_DATASET.YOUR_TABLE`.')

  return parser.parse_known_args()


class ModelHelper():
  def __init__(self):
    args, _ = parse_arguments()
    self.n_neighbors = 8
    self.method = 'largest'
    self.metric = 'euclidean'
    self.contamination = 0.1
    self.bq_table = args.bq_table
    self.dataset = None

  def load_data(self):
    logging.info("Querying vector embeddings from BigQuery...")

    client = bigquery.Client()
    sql = f"""
      SELECT *
      FROM `{self.bq_table}`
      """
    df = client.query_and_wait(sql).to_dataframe()
    self.dataset = np.stack(df['embedding'].to_numpy())

  def train_model(self):
    logging.info("Training KNN model...")

    model = KNN(
        n_neighbors=self.n_neighbors,
        method=self.method,
        metric=self.metric,
        contamination=self.contamination,
    )
    model.fit(self.dataset)

    logging.info("KNN model trained successfully! Saving model...")

    model_pickled_filename = 'knn_model.pkl'
    with open(model_pickled_filename, 'wb') as f:
      pickle.dump(model, f)


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)

  helper = ModelHelper()
  helper.load_data()
  helper.train_model()
