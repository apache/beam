# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import unittest2

from apitools.gen import gen_client
from apitools.gen import test_utils


def GetSampleClientPath(api_name, *path):
    return os.path.join(os.path.dirname(__file__), api_name + '_sample', *path)


def _GetContent(file_path):
    with open(file_path) as f:
        return f.read()


@test_utils.RunOnlyOnPython27
class ClientGenCliTest(unittest2.TestCase):

    def _CheckGeneratedFiles(self, api_name, api_version):
        prefix = api_name + '_' + api_version
        with test_utils.TempDir() as tmp_dir_path:
            gen_client.main([
                gen_client.__file__,
                '--generate_cli',
                '--init-file', 'empty',
                '--infile',
                GetSampleClientPath(api_name, prefix + '.json'),
                '--outdir', tmp_dir_path,
                '--overwrite',
                '--root_package', api_name,
                'client'
            ])
            expected_files = (
                set([prefix + '.py']) |  # CLI files
                set([prefix + '_client.py',
                     prefix + '_messages.py',
                     '__init__.py']))
            self.assertEquals(expected_files, set(os.listdir(tmp_dir_path)))
            for expected_file in expected_files:
                self.assertMultiLineEqual(
                    _GetContent(GetSampleClientPath(
                        api_name, prefix, expected_file)),
                    _GetContent(os.path.join(tmp_dir_path, expected_file)))

    def testGenClient_DnsDoc(self):
        self._CheckGeneratedFiles('dns', 'v1')

    def testGenClient_IamDoc(self):
        self._CheckGeneratedFiles('iam', 'v1')

    def testGenClient_StorageDoc(self):
        self._CheckGeneratedFiles('storage', 'v1')
