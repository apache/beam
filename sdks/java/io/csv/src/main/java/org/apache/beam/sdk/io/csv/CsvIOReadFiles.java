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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Skeleton for error handling in CsvIO that transforms a {@link FileIO.ReadableFile} into the
 * result of parsing.
 */
// TODO(https://github.com/apache/beam/issues/31736): Plan completion in future PR after
// dependencies are completed.
class CsvIOReadFiles<T> extends PTransform<PCollection<FileIO.ReadableFile>, CsvIOParseResult<T>> {
  /** Stores required parameters for parsing. */
  private final CsvIOParseConfiguration.Builder configBuilder;

  CsvIOReadFiles(CsvIOParseConfiguration.Builder configBuilder) {
    this.configBuilder = configBuilder;
  }

  /** {@link PTransform} that parses and relays the filename associated with each error. */
  // TODO: complete expand method to unsure parsing from FileIO.ReadableFile to CsvIOParseResult.
  @Override
  public CsvIOParseResult<T> expand(PCollection<FileIO.ReadableFile> input) {
    // TODO(https://github.com/apache/beam/issues/31736): Needed to prevent check errors, will
    // remove with future PR.
    configBuilder.build();
    TupleTag<T> outputTag = new TupleTag<>();
    TupleTag<CsvIOParseError> errorTag = new TupleTag<>();
    Pipeline p = input.getPipeline();
    PCollectionTuple tuple = PCollectionTuple.empty(p);
    return CsvIOParseResult.of(outputTag, errorTag, tuple);
  }
}
