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

"""The file defines global variables."""

PROJECT_ID = ""
REGION = "us-central1"
# Subscription for PubSub Topic
SUBSCRIPTION_ID = f"projects/{PROJECT_ID}/subscriptions/newsgroup-dataset-subscription"
JOB_NAME = "anomaly-detection-hdbscan"
NUM_WORKERS = 1
EMAIL_ADDRESS = "xyz@gmail.com"

TABLE_SCHEMA = {
    "fields": [
        {
            "name": "text", "type": "STRING", "mode": "NULLABLE"
        },
        {
            "name": "id", "type": "STRING", "mode": "NULLABLE"
        },
        {
            "name": "cluster", "type": "INTEGER", "mode": "NULLABLE"
        },
    ]
}

TABLE_URI = f"{PROJECT_ID}:deliverables_ml6.anomaly-detection"
MODEL_NAME = "sentence-transformers-stsb-distilbert-base"
TOKENIZER_NAME = "sentence-transformers/stsb-distilbert-base"
MODEL_STATE_DICT_PATH = f"gs://{PROJECT_ID}-ml-examples/{MODEL_NAME}/pytorch_model.bin"
MODEL_CONFIG_PATH = TOKENIZER_NAME

CLUSTERING_MODEL_PATH = (
    f"gs://{PROJECT_ID}-ml-examples/anomaly-detection/clustering.joblib")
