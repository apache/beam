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

"""A pipeline that reads an S3 file and logs each line, using AWS credentials
stored in Google Cloud Secret Manager.

Usage (DirectRunner):
  python -m apache_beam.examples.secret_manager.aws_credentials_s3_pipeline \
      --input=s3://my-bucket/my-file.txt \
      --aws_region_name=us-east-1 \
      --aws_credentials_secret_manager=GoogleCloudSecretManager \
      --aws_access_key_id='{"name": "my-aws-key-id", "project": "my-project"}' \
      --aws_secret_access_key='{"name": "my-aws-secret-key", "project": "my-project"}'

Usage (DataflowRunner): add the following flags to the command above.
      --runner=DataflowRunner \
      --project=my-project \
      --region=us-central1 \
      --temp_location=gs://my-gcs-bucket/temp \
      --requirements_file=apache_beam/examples/secret_manager/aws_credentials_requirements.txt

where aws_credentials_requirements.txt (in the same directory as this example)
lists the packages the workers need: boto3 and google-cloud-secret-manager.

Required permissions:
  * The AWS IAM user that owns the access key needs ``s3:ListBucket`` on the
    bucket (``arn:aws:s3:::my-bucket``, used to match the input path) and
    ``s3:GetObject`` on the object (``arn:aws:s3:::my-bucket/my-file.txt``),
    for example through the ``AmazonS3ReadOnlyAccess`` managed policy or a
    narrower custom policy. If the object is encrypted with a customer managed
    KMS key, it also needs ``kms:Decrypt`` on that key.
  * The Google Cloud account that resolves the credentials needs the Secret
    Manager Secret Accessor role (``roles/secretmanager.secretAccessor``) on
    the two secrets that hold the access key ID and secret access key. The
    credentials are resolved both when the pipeline is constructed and where
    the file is read, so this is the account running the script and, on
    Dataflow, also the worker service account.
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def main(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', required=True, help='S3 path to read.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
    _ = (
        p
        | 'Read' >> beam.io.ReadFromText(known_args.input)
        | 'Log' >> beam.Map(logging.info))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
