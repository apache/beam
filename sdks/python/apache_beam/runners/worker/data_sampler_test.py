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

# pytype: skip-file

import unittest

# from apache_beam.coders import FastPrimitivesCoder
from apache_beam.runners.worker.data_sampler import DataSampler
from apache_beam.runners.worker.data_sampler import OutputSampler


class DataSamplerTest(unittest.TestCase):
  def test_give_me_a_name(self):
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    output_sampler = data_sampler.sample_output('a' , '1', coder)
    output_sampler.sample('a')

    self.assertEquals(data_sampler.samples(),
                      {'1': [coder.encode('a')]})

if __name__ == '__main__':
  unittest.main()
