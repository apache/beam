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

 # Tests for generate_roles.py

import unittest
from unittest.mock import MagicMock
import sys
import types
import generate_roles

# Patch yaml and google.cloud imports before importing the script
sys.modules['yaml'] = MagicMock()
sys.modules['google.cloud'] = types.SimpleNamespace(iam_admin_v1=MagicMock())
sys.modules['google.api_core'] = types.SimpleNamespace(exceptions=MagicMock())

class TestGenerateRoles(unittest.TestCase):
    def test_filter_permissions(self):
        perms = [
            'compute.instances.create',
            'compute.instances.delete',
            'storage.buckets.create',
            'storage.buckets.delete',
            'storage.objects.get',
            'bigquery.tables.get',
            'bigquery.tables.delete',
        ]
        allowed = ['storage', 'bigquery']
        denied = ['delete']
        filtered = generate_roles.filter_permissions(perms, allowed, denied)
        self.assertIn('storage.buckets.create', filtered)
        self.assertIn('storage.objects.get', filtered)
        self.assertIn('bigquery.tables.get', filtered)
        self.assertNotIn('storage.buckets.delete', filtered)
        self.assertNotIn('bigquery.tables.delete', filtered)
        self.assertNotIn('compute.instances.create', filtered)
        self.assertNotIn('compute.instances.delete', filtered)

    def test_generate_role(self):
        perms = {'a.b.c', 'd.e.f'}
        role = generate_roles.generate_role('test_role', perms)
        self.assertEqual(role['role_id'], 'test_role')
        self.assertEqual(role['title'], 'test_role')
        self.assertEqual(role['stage'], 'GA')
        self.assertIn('a.b.c', role['permissions'])
        self.assertIn('d.e.f', role['permissions'])

    def test_write_role_yaml(self):
        import tempfile
        import os
        role_data = {
            'role_id': 'test_role',
            'title': 'test_role',
            'stage': 'GA',
            'description': 'desc',
            'permissions': ['a.b.c', 'd.e.f'],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, 'role.yaml')
            generate_roles.ASF_LICENSE_HEADER = ''  # Avoid header for test
            generate_roles.write_role_yaml(filename, role_data)
            with open(filename) as f:
                content = f.read()
            self.assertIn('role_id', content)
            self.assertIn('a.b.c', content)
            self.assertIn('d.e.f', content)

if __name__ == '__main__':
    unittest.main()
