<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

## Jinja % include Pipeline

This example leverages the `% include` Jinja directive by having one main
pipeline and then submodules for each transformed used.

General setup:
```sh
export PIPELINE_FILE=apache_beam/yaml/examples/transforms/jinja/wordCount.yaml
export KINGLEAR="gs://dataflow-samples/shakespeare/kinglear.txt"
export TEMP_LOCATION="gs://MY-BUCKET/wordCounts/"

cd <PATH_TO_BEAM_REPO>/beam/sdks/python
```

Multiline Run Example:
```sh
python -m apache_beam.yaml.main \
  --yaml_pipeline_file=apache_beam/yaml/examples/transforms/jinja/include/wordCount.yaml \
  --jinja_variables='{
    "readFromText": {"path": "'"${KINGLEAR}"'"},
    "mapToFields_split": {
      "language": "python",
      "fields": {
        "value": "1"
      }
    },
    "explode": {"fields": "word"},
    "combine": {
      "group_by": "word",
      "combine": {"value": "sum"}
    },
    "mapToFields_count": {
      "language": "python",
      "fields": {"output": "word + \" - \" + str(value)"}
    },
    "writeToText": {"path": "'"${TEMP_LOCATION}"'"}
  }'
```

Single Line Run Example:
```sh
python -m apache_beam.yaml.main --yaml_pipeline_file=apache_beam/yaml/examples/transforms/jinja/include/wordCount.yaml --jinja_variables='{"readFromText": {"path": "gs://dataflow-samples/shakespeare/kinglear.txt"}, "mapToFields_split": {"language": "python", "fields":{"value":"1"}}, "explode":{"fields":"word"}, "combine":{"group_by":"word", "combine":{"value":"sum"}}, "mapToFields_count":{"language": "python", "fields":{"output":"word + \" - \" + str(value)"}}, "writeToText":{"path":"${TEMP_LOCATION}"}}'
```

