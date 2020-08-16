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
package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Mocked table for bounded data sources. */
@Experimental
public class TestBoundedTable extends TestTable {
  /** rows written to this table. */
  private static final ConcurrentLinkedQueue<Row> CONTENT = new ConcurrentLinkedQueue<>();
  /** rows flow out from this table. */
  private final List<Row> rows = new ArrayList<>();

  public TestBoundedTable(Schema beamSchema) {
    super(beamSchema);
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return BeamTableStatistics.createBoundedTableStatistics((double) rows.size());
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  /**
   * Convenient way to build a mocked bounded table.
   *
   * <p>e.g.
   *
   * <pre>{@code
   * TestUnboundedTable
   *   .of(Types.BIGINT, "order_id",
   *       Types.INTEGER, "site_id",
   *       Types.DOUBLE, "price",
   *       Types.TIMESTAMP, "order_time")
   * }</pre>
   */
  public static TestBoundedTable of(final Object... args) {
    return new TestBoundedTable(TestTableUtils.buildBeamSqlSchema(args));
  }

  /** Build a mocked bounded table with the specified type. */
  public static TestBoundedTable of(final Schema type) {
    return new TestBoundedTable(type);
  }

  /**
   * Add rows to the builder.
   *
   * <p>Sample usage:
   *
   * <pre>{@code
   * addRows(
   *   1, 3, "james", -- first row
   *   2, 5, "bond"   -- second row
   *   ...
   * )
   * }</pre>
   */
  public TestBoundedTable addRows(@Nullable Object... args) {
    List<Row> rows = TestTableUtils.buildRows(getSchema(), Arrays.asList(args));
    this.rows.addAll(rows);
    return this;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(
            "MockedBoundedTable_Reader_" + COUNTER.incrementAndGet(),
            Create.of(rows).withRowSchema(schema))
        .setRowSchema(getSchema());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    input.apply(
        ParDo.of(
            new DoFn<Row, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                CONTENT.add(c.element());
              }

              @Teardown
              public void close() {
                CONTENT.clear();
              }
            }));
    return PDone.in(input.getPipeline());
  }
}
