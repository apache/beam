<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# I/O Read

When you create a pipeline, you often need to read data from some external source, such as a file
or a database. Likewise, you may want your pipeline to output its result data to an external
storage system. Beam provides read and write transforms for a number of common data storage types.

Package [textio](https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/io/textio)
contains transforms for reading and writing text files.
[textio.Read](https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/io/textio#Read) reads a set of files and
returns the lines as a PCollection<string>.

**Kata:** Read the 'countries.txt' file and convert each country name into uppercase.

<div class="hint">
  Use the <a href="https://godoc.org/github.com/apache/beam/sdks/go/pkg/beam/io/textio#Read">
  textio.Read</a> method.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#pipeline-io-reading-data">
  "Reading input data"</a> section for more information.
</div>
