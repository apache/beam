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
package org.apache.beam.sdk.extensions.sql;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

/**
 * A seekable table converts a JOIN operator to an inline lookup. It's triggered by {@code SELECT *
 * FROM FACT_TABLE JOIN LOOKUP_TABLE ON ...}.
 */
public interface BeamSqlSeekableTable extends Serializable {
  /**
   * prepare the instance.
   *
   * @param joinSubsetType joining subset schema
   */
  default void setUp(Schema joinSubsetType) {}

  default void startBundle(
      DoFn<Row, Row>.StartBundleContext context, PipelineOptions pipelineOptions) {}

  default void finishBundle(
      DoFn<Row, Row>.FinishBundleContext context, PipelineOptions pipelineOptions) {}

  /** return a list of {@code Row} with given key set. */
  List<Row> seekRow(Row lookupSubRow);

  /** cleanup resources of the instance. */
  default void tearDown() {}
}
