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
package org.apache.beam.sdk.extensions.sql.meta;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;

/** This interface defines a Beam Sql Table. */
public interface BeamSqlTable {
  /** create a {@code PCollection<Row>} from source. */
  PCollection<Row> buildIOReader(PBegin begin);

  /** create a {@code PCollection<Row>} from source with predicate and/or project pushed-down. */
  PCollection<Row> buildIOReader(PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames);

  /** create a {@code IO.write()} instance to write to target. */
  POutput buildIOWriter(PCollection<Row> input);

  /** Generate an IO implementation of {@code BeamSqlTableFilter} for predicate push-down. */
  BeamSqlTableFilter constructFilter(List<RexNode> filter);

  /** Whether project push-down is supported by the IO API. */
  ProjectSupport supportsProjects();

  /** Whether this table is bounded (known to be finite) or unbounded (may or may not be finite). */
  PCollection.IsBounded isBounded();

  /** Get the schema info of the table. */
  Schema getSchema();

  /**
   * Estimates the number of rows or the rate for unbounded Tables. If it is not possible to
   * estimate the row count or rate it will return BeamTableStatistics.BOUNDED_UNKNOWN.
   */
  BeamTableStatistics getTableStatistics(PipelineOptions options);

  /**
   * Returns the table to read for one reference in FROM that has a {@code WITH} clause, such as
   * {@code SELECT * FROM t WITH ('key' = 'value')}.
   *
   * <p>By default, tables reject {@code WITH}. Override this method to support it in queries
   * reading from your connector. Override the {@link #forWrite} method to support it for INSERT
   * targets. Return a new table that has the given options applied to it. It may have a different
   * schema, boundedness or push-down support than this one.
   *
   * <p>{@code options} has the clause's keys in the order they were written. A string literal
   * arrives as a String, an integer as a Long, any other number as a Double, TRUE or FALSE as a
   * Boolean, and ARRAY[...] and MAP[...] as a List and a Map.
   *
   * <p>Planning calls this method several times per reference, so keep it cheap and don't call the
   * catalog. Don't change this table or its {@link Table} either.
   *
   * <p>For an example, see {@code IcebergTable#forWrite} in sdks/java/extensions/sql/iceberg.
   *
   * @throws IllegalArgumentException for an unknown option key or a value of the wrong type
   * @throws UnsupportedOperationException if this table does not support {@code WITH} for reads
   */
  default BeamSqlTable forRead(Map<String, Object> options) {
    throw new UnsupportedOperationException(
        String.format("%s does not support WITH options for reads", getClass().getSimpleName()));
  }

  /**
   * Returns the table to write for an INSERT target that has a {@code WITH} clause, such as {@code
   * INSERT INTO t WITH ('key' = 'value') SELECT ...}. Everything in {@link #forRead} applies here
   * too.
   */
  default BeamSqlTable forWrite(Map<String, Object> options) {
    throw new UnsupportedOperationException(
        String.format("%s does not support WITH options for writes", getClass().getSimpleName()));
  }

  /**
   * Returns the metadata columns that {@code CHANGES(...)} can read from this table, such as the
   * change type. A query can select them by name, and Beam SQL passes the ones it names to {@link
   * #forChanges}. Returns none by default.
   */
  default List<String> getChangeMetadataColumns() {
    return Collections.emptyList();
  }

  /**
   * Returns the table to read for one {@code CHANGES(...)} call in FROM, such as {@code SELECT *
   * FROM CHANGES(t, 5, 10)}: the changes committed in the {@code changes} range, read with the
   * {@code WITH} options, if any. Both bounds are inclusive. Everything in {@link #forRead} applies
   * here too.
   *
   * <p>{@code metadataColumns} holds the metadata columns that the query names and that the table
   * doesn't already return for these options. Add them to the returned table's columns. Beam SQL
   * keeps them out of {@code SELECT *}.
   *
   * <p>The parser will already reject malformed bounds, such as a negative version or an invalid
   * timestamp format. Beyond that, it's up to the CDC source to validate the range against the
   * table. Batch and streaming reads should both follow these rules:
   *
   * <ul>
   *   <li>A version bound must name a version that exists, and a start version must be the ancestor
   *       of an end version (or equal to it). Otherwise the read should fail.
   *   <li>A timestamp bound can be any time. The read starts at the first version committed at or
   *       after the start, and stops at the last version committed at or before the end.
   *   <li>A batch read that holds no versions returns no rows. Same applies to a streaming read
   *       with a closed range.
   *   <li>A streaming read with an open end keeps reading, even if nothing has been committed in
   *       its range yet.
   * </ul>
   *
   * @throws UnsupportedOperationException if this table does not support {@code CHANGES(...)}
   */
  default BeamSqlTable forChanges(
      ChangeRange changes, Map<String, Object> options, List<String> metadataColumns) {
    throw new UnsupportedOperationException(
        String.format("%s does not support CHANGES(...)", getClass().getSimpleName()));
  }
}
