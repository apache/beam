/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as beam from "../../apache_beam";
import * as external from "../transforms/external";

export class ReadFromText extends beam.AsyncPTransform<
  beam.Root,
  beam.PCollection<string>
> {
  constructor(private filePattern: string) {
    super();
  }

  async asyncExpand(root: beam.Root) {
    return await root.asyncApply(
      new external.RawExternalTransform<
        beam.PValue<any>,
        beam.PCollection<any>
      >(
        "beam:transforms:python:fully_qualified_named",
        {
          constructor: "apache_beam.io.ReadFromText",
          kwargs: { file_pattern: this.filePattern },
        },
        // python apache_beam/runners/portability/expansion_service_main.py --fully_qualified_name_glob='*' --port 4444 --environment_type='beam:env:embedded_python:v1'
        "localhost:4444"
      )
    );
  }
}
