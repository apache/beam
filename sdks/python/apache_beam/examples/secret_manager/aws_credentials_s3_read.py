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

"""Reads an S3 file outside of a pipeline, using AWS credentials stored in
Google Cloud Secret Manager.

Usage:
  python -m apache_beam.examples.secret_manager.aws_credentials_s3_read \
      --input=s3://my-bucket/my-file.txt \
      --aws_region_name=us-east-1 \
      --aws_credentials_secret_manager=GoogleCloudSecretManager \
      --aws_access_key_id='{"name": "my-aws-key-id", "project": "my-project"}' \
      --aws_secret_access_key='{"name": "my-aws-secret-key", "project": "my-project"}'

Required permissions:
  * The AWS IAM user that owns the access key needs ``s3:GetObject`` on the
    object (e.g. ``arn:aws:s3:::my-bucket/my-file.txt``), for example through
    the ``AmazonS3ReadOnlyAccess`` managed policy or a narrower custom policy.
    If the object is encrypted with a customer managed KMS key, it also needs
    ``kms:Decrypt`` on that key.
  * The Google Cloud account running this script (e.g. your Application
    Default Credentials) needs the Secret Manager Secret Accessor role
    (``roles/secretmanager.secretAccessor``) on the two secrets that hold the
    access key ID and secret access key.
"""

import argparse

from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions


def main(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', required=True, help='S3 path to read.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # The S3 filesystem resolves the --aws_* credentials through the secret
  # manager given by --aws_credentials_secret_manager.
  FileSystems.set_options(PipelineOptions(pipeline_args))
  with FileSystems.open(known_args.input) as f:
    print(f.read().decode('utf-8'))


if __name__ == '__main__':
  main()
