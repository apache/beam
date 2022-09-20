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

import argparse
import sys

import apache_beam as beam
from apache_beam.io.gcp.pubsub import WriteToPubSub

import config as cfg
from pipeline.options import get_pipeline_options
from pipeline.utils import AssignUniqueID, ConvertToPubSubMessage, get_dataset


def parse_arguments(argv):
  parser = argparse.ArgumentParser(description="write-to-pubsub")

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


def run():
  args = parse_arguments(sys.argv)
  pipeline_options = get_pipeline_options(
      job_name=cfg.JOB_NAME,
      num_workers=cfg.NUM_WORKERS,
      project=args.project,
      mode=args.mode,
  )
  train_categories = ["joy", "love", "fear"]
  test_categories = train_categories + ["sadness"]
  train_data, train_labels = get_dataset(train_categories)

  with beam.Pipeline(options=pipeline_options) as pipeline:
    docs = (
        pipeline
        | "Load Documents" >> beam.Create(train_data)
        | "Assign unique key" >> beam.ParDo(AssignUniqueID()))
    _ = (
        docs
        | "Convert to PubSub Message" >> beam.ParDo(ConvertToPubSubMessage())
        | "Write to PubSub" >> WriteToPubSub(
            topic=cfg.TOPIC_ID, with_attributes=True))


if __name__ == "__main__":
  run()
