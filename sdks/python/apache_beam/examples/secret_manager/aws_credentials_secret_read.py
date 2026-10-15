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

"""Reads a secret from AWS Secrets Manager outside of a pipeline, using AWS
credentials stored in Google Cloud Secret Manager.

Usage:
  python -m apache_beam.examples.secret_manager.aws_credentials_secret_read \
      --secret_id=my-aws-secret \
      --secret_arn=arn:aws:secretsmanager:us-east-1:<account-id>:secret:my-aws-secret-AbCdEf \
      --aws_region_name=us-east-1 \
      --aws_credentials_secret_manager=GoogleCloudSecretManager \
      --aws_access_key_id='{"name": "my-aws-key-id", "project": "my-project"}' \
      --aws_secret_access_key='{"name": "my-aws-secret-key", "project": "my-project"}'

Required permissions:
  * The AWS IAM user that owns the access key needs
    ``secretsmanager:GetSecretValue`` on the secret (e.g.
    ``arn:aws:secretsmanager:us-east-1:<account-id>:secret:my-aws-secret-*``),
    for example through the ``AWSSecretsManagerClientReadOnlyAccess`` managed
    policy or a narrower custom policy. If the secret is encrypted with a
    customer managed KMS key instead of the default ``aws/secretsmanager`` key,
    it also needs ``kms:Decrypt`` on that key.
  * The Google Cloud account running this script (e.g. your Application
    Default Credentials) needs the Secret Manager Secret Accessor role
    (``roles/secretmanager.secretAccessor``) on the two secrets that hold the
    access key ID and secret access key.
"""

import argparse

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.secret import Secret


def main(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--secret_id',
      required=True,
      help='Name of the secret in AWS Secrets Manager.')
  parser.add_argument(
      '--secret_arn',
      help='Optional ARN of the secret in AWS Secrets Manager.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  options = PipelineOptions(pipeline_args)

  # The AWS Secrets Manager client uses the --aws_* options, with credentials
  # resolved through --aws_credentials_secret_manager.

  # Option 1: a 'type:<secret_type>;<param>:<value>' secret string.
  secret = Secret.parse_secret_option(
      f'type:awssecret;name:{known_args.secret_id}', options)
  print(f'Retrieved secret of length {len(secret.get_str())}')

  # Option 2: a JSON spec and a secret manager name.
  secret = Secret.from_json(
      f'{{"name": "{known_args.secret_id}"}}', 'AwsSecretsManager', options)
  print(f'Retrieved secret of length {len(secret.get_str())}')

  # Options 3 and 4: the same as options 1 and 2, but identifying the secret by
  # its ARN.
  if known_args.secret_arn:
    secret = Secret.parse_secret_option(
        f'type:awssecret;arn:{known_args.secret_arn}', options)
    print(f'Retrieved secret of length {len(secret.get_str())}')

    secret = Secret.from_json(
        f'{{"arn": "{known_args.secret_arn}"}}', 'AwsSecretsManager', options)
    print(f'Retrieved secret of length {len(secret.get_str())}')


if __name__ == '__main__':
  main()
