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
import unittest

import apache_beam as beam
from apache_beam.runners.dask.dask_runner import DaskRunner
from apache_beam.testing import test_pipeline


class DaskRunnerRunPipelineTest(unittest.TestCase):
    """Test class used to introspect the dask runner via a debugger."""

    def test_create(self):
        p = test_pipeline.TestPipeline(runner=DaskRunner())
        _ = p | beam.Create([1])
        p.run()

    def test_create_and_map(self):
        def double(x):
            return x * 2

        p = test_pipeline.TestPipeline(runner=DaskRunner())
        _ = p | beam.Create([1]) | beam.Map(double)
        p.run()

    def test_create_map_and_groupby(self):
        def double(x):
            return x * 2, x

        p = test_pipeline.TestPipeline(runner=DaskRunner())
        _ = p | beam.Create([1]) | beam.Map(double) | beam.GroupByKey()
        p.run()


if __name__ == '__main__':
    unittest.main()
