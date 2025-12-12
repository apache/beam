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
import unittest
from unittest import mock

from apache_beam.yaml import kms
from apache_beam.yaml import yaml_transform

class KmsTest(unittest.TestCase):
  
  def test_decrypt_value(self):
    with mock.patch('apache_beam.yaml.kms.kms') as mock_kms_module:
      mock_client = mock.Mock()
      mock_kms_module.KeyManagementServiceClient.return_value = mock_client
      mock_response = mock.Mock()
      mock_response.plaintext = b'my_secret'
      mock_client.decrypt.return_value = mock_response

      ciphertext = base64.b64encode(b'encrypted_secret').decode('utf-8')
      key_name = 'projects/p/locations/l/keyRings/k/cryptoKeys/c'
      
      plaintext = kms.decrypt_value(ciphertext, key_name)
      
      self.assertEqual(plaintext, 'my_secret')
      mock_client.decrypt.assert_called_once()
      args, kwargs = mock_client.decrypt.call_args
      self.assertEqual(kwargs['request']['name'], key_name)
      self.assertEqual(kwargs['request']['ciphertext'], b'encrypted_secret')

  def test_preprocess_encryption(self):
    with mock.patch('apache_beam.yaml.kms.decrypt_value') as mock_decrypt:
      mock_decrypt.return_value = 'decrypted_password'
      
      spec = {
          'type': 'MyTransform',
          'config': {
              'username': 'user',
              'password': 'encrypted_password'
          },
          'encryption': {
              'key': 'projects/p/locations/l/keyRings/k/cryptoKeys/c',
              'fields': ['password']
          }
      }
      
      processed_spec = yaml_transform.preprocess_encryption(spec)
      
      self.assertNotIn('encryption', processed_spec)
      self.assertEqual(processed_spec['config']['password'], 'decrypted_password')
      mock_decrypt.assert_called_once_with('encrypted_password', 'projects/p/locations/l/keyRings/k/cryptoKeys/c')

  def test_preprocess_encryption_missing_key(self):
    spec = {
        'type': 'MyTransform',
        'config': {'p': 'v'},
        'encryption': {
            'fields': ['p']
        }
    }
    with self.assertRaisesRegex(ValueError, "Encryption block missing 'key'"):
      yaml_transform.preprocess_encryption(spec)

  def test_preprocess_encryption_missing_field(self):
    spec = {
        'type': 'MyTransform',
        'config': {'other': 'v'},
        'encryption': {
            'key': 'k',
            'fields': ['missing_field']
        }
    }
    with self.assertRaisesRegex(ValueError, "Encrypted field 'missing_field' not found"):
      yaml_transform.preprocess_encryption(spec)

if __name__ == '__main__':
  unittest.main()
