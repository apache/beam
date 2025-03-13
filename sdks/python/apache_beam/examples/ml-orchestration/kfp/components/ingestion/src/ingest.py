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

"""Ingestion function that fetches data from one file and simply copies it to another."""

import argparse
import time
from pathlib import Path


def parse_args():
  """Parse ingestion arguments."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--ingested-dataset-path",
      type=str,
      help="Path to save the ingested dataset to.",
      required=True)
  parser.add_argument(
      "--base-artifact-path",
      type=str,
      help="Base path to store pipeline artifacts.",
      required=True)
  return parser.parse_args()


def ingest_data(ingested_dataset_path: str, base_artifact_path: str):
  """Data ingestion step that returns an uri
  to the data it has 'ingested' as jsonlines.

  Args:
    data_ingestion_target (str): uri to the data that was scraped and
    ingested by the component"""
  # timestamp as unique id for the component execution
  timestamp = int(time.time())

  # create directory to store the actual data
  target_path = f"{base_artifact_path}/ingestion/ingested_dataset_{timestamp}.jsonl"
  # if the target path is a google cloud storage path convert the path to the gcsfuse path
  target_path_gcsfuse = target_path.replace("gs://", "/gcs/")
  Path(target_path_gcsfuse).parent.mkdir(parents=True, exist_ok=True)

  with open(target_path_gcsfuse, 'w') as f:
    f.writelines([
        """{"image_id": 318556, "id": 255, "caption": "An angled view of a beautifully decorated bathroom.", "image_url": "http://farm4.staticflickr.com/3133/3378902101_3c9fa16b84_z.jpg", "image_name": "COCO_train2014_000000318556.jpg", "image_license": "Attribution-NonCommercial-ShareAlike License"}\n""",
        """{"image_id": 476220, "id": 314, "caption": "An empty kitchen with white and black appliances.", "image_url": "http://farm7.staticflickr.com/6173/6207941582_b69380c020_z.jpg", "image_name": "COCO_train2014_000000476220.jpg", "image_license": "Attribution-NonCommercial License"}\n""",
        """{"image_id": 134754, "id": 425, "caption": "Two people carrying surf boards on a beach.", "image_url": "http://farm9.staticflickr.com/8500/8398513396_b6a1f11a4b_z.jpg", "image_name": "COCO_train2014_000000134754.jpg", "image_license": "Attribution-NonCommercial-NoDerivs License"}"""
    ])

  # the directory where the output file is created may or may not exists
  # so we have to create it.
  # KFP v1 components can only write output to files. The output of this
  # component is written to ingested_dataset_path and contains the path
  # of the actual ingested data
  Path(ingested_dataset_path).parent.mkdir(parents=True, exist_ok=True)
  with open(ingested_dataset_path, 'w') as f:
    f.write(target_path)


if __name__ == "__main__":
  args = parse_args()
  ingest_data(**vars(args))
