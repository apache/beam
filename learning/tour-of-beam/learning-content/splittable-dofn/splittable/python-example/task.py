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

# beam-playground:
#   name: splittable-dofn
#   description: Splittable DoFn example.
#   multifile: false
#   context_line: 64
#   categories:
#     - Quickstart
#   complexity: BASIC
#   tags:
#     - hellobeam

import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io import restriction_trackers
from apache_beam.io.restriction_trackers import OffsetRange


class SplitLinesFn(beam.RestrictionProvider):
    def create_tracker(self, restriction):
        return restriction_trackers.OffsetRestrictionTracker(restriction)

    def initial_restriction(self, element):
        return OffsetRange(0, len(element.split()))

    def restriction_size(self, element, restriction):
        return restriction.size()

    def split(self, element, restriction):
        start = restriction.start
        size = restriction.stop - restriction.start
        split_size_bytes = size // 5
        while start < restriction.stop:
            split_end = start + split_size_bytes
            if split_end >= restriction.stop:
                split_end = restriction.stop
            yield OffsetRange(start, split_end)
            start = split_end

class MyFileSource(beam.DoFn):
    def process(self, element, restriction_tracker=DoFn.RestrictionParam(SplitLinesFn())):
        input = element.split()
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
            yield cur, input[cur]
            cur += 1


with beam.Pipeline() as p:
  lines = (p | 'Create' >> beam.Create(["Lorem Ipsum is simply dummy text of the printing and typesetting industry. "
                                        "Lorem Ipsum has been the industry's standard dummy text ever since the "
                                        "1500s, when an unknown printer took a galley of type and scrambled it to "
                                        "make a type specimen book. It has survived not only five centuries, "
                                        "but also the leap into electronic typesetting, remaining essentially "
                                        "unchanged. It was popularised in the 1960s with the release of Letraset "
                                        "sheets containing Lorem Ipsum passages, and more recently with desktop "
                                        "publishing software like Aldus PageMaker including versions of Lorem Ipsum."])
             | beam.ParDo(MyFileSource())
             | 'Print' >> beam.Map(print))
