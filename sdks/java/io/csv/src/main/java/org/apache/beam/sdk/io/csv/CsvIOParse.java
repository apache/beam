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

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * CsvIO master class that takes an input of {@link PCollection<String>} and outputs custom type
 * {@link PCollection<T>}.
 */
// TODO(https://github.com/apache/beam/issues/31877): Plan for implementation after all dependencies
// are completed.
class CsvIOParse<T> extends PTransform<PCollection<String>, PCollection<T>> {

  /** Stores required parameters for parsing. */
  private final CsvIOParseConfiguration.Builder configBuilder;

  CsvIOParse(CsvIOParseConfiguration.Builder configBuilder) {
    this.configBuilder = configBuilder;
  }

  @Override
  public PCollection<T> expand(PCollection<String> input) {
    // TODO: Remove dependency to build CsvIOConfiguration with future PR, needed to pass checks.
    configBuilder.build();
    return input.apply(ParDo.of(new DoFn<String, T>() {}));
  }

  /** Parses to custom type not specified under {@link Schema.FieldType}. */
  // TODO(https://github.com/apache/beam/issues/31875): Implement method.
  void withCustomRecordParsing() {}

  /** Parses cell to emit the value, as well as potential errors with filename. */
  // TODO(https://github.com/apache/beam/issues/31876):Implement method.
  Object parseCell(String cell, Schema.Field field) {
    return "";
  }
}
