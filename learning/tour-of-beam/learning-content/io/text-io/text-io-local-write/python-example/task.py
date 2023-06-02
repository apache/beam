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
#   name: text-io-local-write
#   description: TextIO write local file example.
#   multifile: true
#   files:
#     - name: myfile.txt
#   context_line: 30
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

import apache_beam as beam

def print_lines(line):
    print(line)

p = beam.Pipeline()

data = ['Hello, World!', 'Apache Beam']

p | 'CreateMyData' >> beam.Create(data) | 'WriteMyFile' >> beam.io.WriteToText(file_path_prefix='myfile.txt',shard_name_template='')

p.run().wait_until_finish()