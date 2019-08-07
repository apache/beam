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

import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/** This interface defines a Beam Sql Table. */
public interface BeamSqlTable {
  /** create a {@code PCollection<Row>} from source. */
  PCollection<Row> buildIOReader(PBegin begin);

  /** create a {@code IO.write()} instance to write to target. */
  POutput buildIOWriter(PCollection<Row> input);

  /** Whether this table is bounded (known to be finite) or unbounded (may or may not be finite). */
  PCollection.IsBounded isBounded();

  /** Get the schema info of the table. */
  Schema getSchema();

  /**
   * Estimates the number of rows or the rate for unbounded Tables. If it is not possible to
   * estimate the row count or rate it will return BeamTableStatistics.BOUNDED_UNKNOWN.
   */
  BeamTableStatistics getTableStatistics(PipelineOptions options);
}
