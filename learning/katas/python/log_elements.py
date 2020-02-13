#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import apache_beam as beam


class LogElements(beam.PTransform):

    class _LoggingFn(beam.DoFn):

        def __init__(self, prefix=''):
            super(LogElements._LoggingFn, self).__init__()
            self.prefix = prefix

        def process(self, element, **kwargs):
            print(self.prefix + str(element))
            yield element

    def __init__(self, label=None, prefix=''):
        super(LogElements, self).__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._LoggingFn(self.prefix))
