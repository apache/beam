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
package org.apache.beam.sdk.io.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

/** The gate's three outcomes, and that a refusal holds back only its own table. */
public class TableGateTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final String ORDERS = "default.orders";
  private static final String EVENTS = "default.events";
  private static final String ORDERS_FILE = "gs://lake/export/orders/a.parquet";
  private static final String EVENTS_FILE = "gs://lake/export/events/b.parquet";
  private static final String REASON = "every file schema read was refused";

  @Test
  public void aCommittedTableReleasesItsFiles() {
    PCollectionTuple out =
        gate(
            Arrays.asList(KV.of(ORDERS, SchemaCommitVerdict.committed(7L))),
            Arrays.asList(KV.of(ORDERS, ORDERS_FILE)));

    PAssert.that(out.get(TableGate.RELEASED)).containsInAnyOrder(KV.of(ORDERS, ORDERS_FILE));
    PAssert.that(out.get(TableGate.ERRORS)).empty();
    pipeline.run().waitUntilFinish();
  }

  /** The subtle branch: no verdict means the pre-pass never saw the table, not that it refused. */
  @Test
  public void aTableWithNoVerdictIsReleased() {
    PCollectionTuple out =
        gate(
            Arrays.asList(KV.of(ORDERS, SchemaCommitVerdict.committed(7L))),
            Arrays.asList(KV.of(EVENTS, EVENTS_FILE)));

    PAssert.that(out.get(TableGate.RELEASED)).containsInAnyOrder(KV.of(EVENTS, EVENTS_FILE));
    PAssert.that(out.get(TableGate.ERRORS)).empty();
    pipeline.run().waitUntilFinish();
  }

  /** Fields are read by name: a transposed row would pass an equality check against a twin. */
  @Test
  public void aRefusedTableReportsEveryFileWithTheReasonAndTheTable() {
    PCollectionTuple out =
        gate(
            Arrays.asList(KV.of(ORDERS, SchemaCommitVerdict.refused(REASON))),
            Arrays.asList(KV.of(ORDERS, ORDERS_FILE), KV.of(ORDERS, ORDERS_FILE + "2")));

    PAssert.that(out.get(TableGate.RELEASED)).empty();
    PAssert.that(out.get(TableGate.ERRORS))
        .satisfies(
            rows -> {
              int seen = 0;
              for (Row row : rows) {
                seen++;
                assertEquals(REASON, row.getString("error"));
                assertEquals(ORDERS, row.getString("table"));
                assertTrue(
                    "unexpected file: " + row.getString("file"),
                    ORDERS_FILE.equals(row.getString("file"))
                        || (ORDERS_FILE + "2").equals(row.getString("file")));
              }
              assertEquals(2, seen);
              return null;
            });

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    // files, not tables: two files of one refused table
    assertEquals(2, filesHeldBack(result));
  }

  /** The point of the feature: one table's refusal must not cost another table its files. */
  @Test
  public void oneRefusedTableDoesNotHoldBackAnother() {
    PCollectionTuple out =
        gate(
            Arrays.asList(
                KV.of(ORDERS, SchemaCommitVerdict.refused(REASON)),
                KV.of(EVENTS, SchemaCommitVerdict.committed(3L))),
            Arrays.asList(KV.of(ORDERS, ORDERS_FILE), KV.of(EVENTS, EVENTS_FILE)));

    PAssert.that(out.get(TableGate.RELEASED)).containsInAnyOrder(KV.of(EVENTS, EVENTS_FILE));
    PAssert.that(out.get(TableGate.ERRORS))
        .satisfies(
            rows -> {
              for (Row row : rows) {
                assertEquals(ORDERS, row.getString("table"));
                assertEquals(ORDERS_FILE, row.getString("file"));
              }
              return null;
            });
    pipeline.run().waitUntilFinish();
  }

  private PCollectionTuple gate(
      List<KV<String, SchemaCommitVerdict>> verdicts, List<KV<String, String>> files) {
    PCollectionView<Map<String, SchemaCommitVerdict>> view =
        pipeline
            .apply("Verdicts", Create.of(verdicts).withCoder(verdictCoder()))
            .apply("AsView", View.asMap());
    PCollectionTuple out =
        pipeline
            .apply("Files", Create.of(files))
            .apply(
                ParDo.of(new TableGate(view))
                    .withSideInputs(view)
                    .withOutputTags(TableGate.RELEASED, TupleTagList.of(TableGate.ERRORS)));
    out.get(TableGate.ERRORS).setRowSchema(AddFiles.DYNAMIC_ERROR_SCHEMA);
    return out;
  }

  private static KvCoder<String, SchemaCommitVerdict> verdictCoder() {
    try {
      return KvCoder.of(
          StringUtf8Coder.of(),
          SchemaRegistry.createDefault().getSchemaCoder(SchemaCommitVerdict.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  private static long filesHeldBack(PipelineResult result) {
    long total = 0;
    for (MetricResult<Long> counter :
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(TableGate.class, TableGate.HELD_COUNTER))
                    .build())
            .getCounters()) {
      total += counter.getAttempted();
    }
    return total;
  }
}
