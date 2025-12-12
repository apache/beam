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

import base64
import logging

try:
  from google.cloud import kms
except ImportError:
  kms = None

_LOGGER = logging.getLogger(__name__)


def decrypt_value(ciphertext: str, key_name: str) -> str:
  """Decrypts a ciphertext using Google Cloud KMS.

  Args:
    ciphertext: The base64 encoded ciphertext to decrypt.
    key_name: The resource name of the CryptoKey to use for decryption.
              Format: projects/*/locations/*/keyRings/*/cryptoKeys/*

  Returns:
    The decrypted plaintext string.

  Raises:
    ValueError: If the key_name format is invalid.
    ImportError: If google-cloud-kms is not installed.
    Exception: If decryption fails.
  """
  if not key_name.startswith('projects/'):
    raise ValueError(f'Key name must start with "projects/", got {key_name}')

  if kms is None:
    raise ImportError(
        'google-cloud-kms is required for encryption. '
        'Please install apache-beam[gcp] or `pip install google-cloud-kms`.')

  client = kms.KeyManagementServiceClient()

  # Decode the base64 ciphertext
  try:
    ciphertext_bytes = base64.b64decode(ciphertext)
  except Exception as e:
    raise ValueError(f"Failed to base64 decode ciphertext: {e}") from e

  # Build the request
  request = {
      "name": key_name,
      "ciphertext": ciphertext_bytes,
  }

  # Call the API
  try:
    response = client.decrypt(request=request)
    return response.plaintext.decode('utf-8')
  except Exception as e:
    _LOGGER.error(f"Failed to decrypt value with key {key_name}: {e}")
    raise
