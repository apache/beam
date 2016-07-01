#
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

"""Test gen_client against all the APIs we use regularly."""

import logging
import os
import subprocess
import tempfile

import unittest2

from apitools.gen import test_utils


_API_LIST = [
    'drive.v2',
    'bigquery.v2',
    'compute.v1',
    'storage.v1',
]


class ClientGenerationTest(unittest2.TestCase):

    def setUp(self):
        super(ClientGenerationTest, self).setUp()
        self.gen_client_binary = 'gen_client'

    @test_utils.SkipOnWindows
    @test_utils.RunOnlyOnPython27
    def testGeneration(self):
        for api in _API_LIST:
            with test_utils.TempDir(change_to=True):
                args = [
                    self.gen_client_binary,
                    '--client_id=12345',
                    '--client_secret=67890',
                    '--discovery_url=%s' % api,
                    '--outdir=generated',
                    '--overwrite',
                    'client',
                ]
                logging.info('Testing API %s with command line: %s',
                             api, ' '.join(args))
                retcode = subprocess.call(args)
                if retcode == 128:
                    logging.error('Failed to fetch discovery doc, continuing.')
                    continue
                self.assertEqual(0, retcode)

                with tempfile.NamedTemporaryFile() as out:
                    with tempfile.NamedTemporaryFile() as err:
                        cmdline_args = [
                            os.path.join(
                                'generated', api.replace('.', '_') + '.py'),
                            'help',
                        ]
                        retcode = subprocess.call(
                            cmdline_args, stdout=out, stderr=err)
                        with open(err.name, 'rb') as f:
                            err_output = f.read()
                # appcommands returns 1 on help
                self.assertEqual(1, retcode)
                if 'Traceback (most recent call last):' in err_output:
                    err = '\n======\n%s======\n' % err_output
                    self.fail(
                        'Error raised in generated client:' + err)
