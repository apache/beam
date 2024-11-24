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
package org.apache.beam.sdk.io.csv;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVRecord;

/**
 * A {@link PTransform} that takes an input {@link PCollection<KV<String,CSVRecord>>} and outputs a
 * {@link PCollection<T>} of custom type.
 */
// TODO(https://github.com/apache/beam/issues/31873): implement class after all dependencies are
// completed.
class CsvIOParseKV<T>
    extends PTransform<PCollection<KV<String, Iterable<String>>>, CsvIOParseResult<T>> {

  private final Coder<T> outputCoder;

  private CsvIOParseKV(Coder<T> outputCoder) {
    this.outputCoder = outputCoder;
  }

  // TODO(https://github.com/apache/beam/issues/31873): implement method.
  @Override
  public CsvIOParseResult<T> expand(PCollection<KV<String, Iterable<String>>> input) {
    return CsvIOParseResult.empty(input.getPipeline(), outputCoder);
  }
}
