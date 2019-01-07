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
package org.apache.beam.sdk.io.clickhouse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for atomic/idempotent inserts for {@link ClickHouseIO}. */
@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class AtomicInsertTest extends BaseClickHouseTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  /** With sufficient block size, ClickHouse will atomically insert all or nothing. */
  @Test
  public void testAtomicInsert() throws SQLException {
    int size = 1000000;
    int tryLimit = 10;

    int done = 0;

    // this statement fails with 60% chance for 1M batch size
    executeSql(
        "CREATE TABLE test_atomic_insert ("
            + "  f0 Int64, "
            + "  f1 Int64 MATERIALIZED CAST(if((rand() % "
            + size
            + ") = 0, '', '1') AS Int64)"
            + ") ENGINE=MergeTree ORDER BY (f0)");

    pipeline
        // make sure we get one big bundle
        .apply(RangeBundle.of(size))
        .apply(
            ClickHouseIO.<Row>write(clickHouse.getJdbcUrl(), "test_atomic_insert")
                .withMaxInsertBlockSize(size)
                .withMaxCumulativeBackoff(Duration.millis(1L))
                .withMaxRetries(tryLimit));

    // give it a chance to fail
    done += safeRun() ? 1 : 0;
    done += safeRun() ? 1 : 0;
    done += safeRun() ? 1 : 0;

    long count = executeQueryAsLong("SELECT COUNT(*) FROM test_atomic_insert");

    // each insert is atomic, so we get exactly done * size elements
    assertEquals(((long) done) * size, count);
    assertTrue(count > 0L); // at least one should succeed
  }

  /**
   * With sufficient block size, ClickHouse will atomically insert all or nothing. In the case of
   * replicated tables, it will deduplicate blocks.
   */
  @Test
  public void testIdempotentInsert() throws SQLException {
    int size = 1000000;
    int tryLimit = 10;

    // this statement fails with 60% chance for 1M batch size
    executeSql(
        "CREATE TABLE test_idempotent_insert ("
            + "  f0 Int64, "
            + "  f1 Int64 MATERIALIZED CAST(if((rand() % "
            + size
            + ") = 0, '', '1') AS Int64)"
            + ") ENGINE=ReplicatedMergeTree('/clickHouse/tables/0/test_idempotent_insert', 'replica_0') "
            + "ORDER BY (f0)");

    pipeline
        .apply(RangeBundle.of(size))
        .apply(
            ClickHouseIO.<Row>write(clickHouse.getJdbcUrl(), "test_idempotent_insert")
                .withMaxInsertBlockSize(size)
                .withMaxCumulativeBackoff(Duration.millis(1L))
                .withMaxRetries(tryLimit));

    // give it a chance to fail
    safeRun();
    safeRun();
    safeRun();

    long count = executeQueryAsLong("SELECT COUNT(*) FROM test_idempotent_insert");

    // inserts should be deduplicated, so we get exactly `size` elements
    assertEquals(size, count);
    assertTrue(count > 0L); // at least one should succeed
  }

  private static class RangeBundle extends PTransform<PBegin, PCollection<Row>> {

    private final int size;

    private RangeBundle(int size) {
      this.size = size;
    }

    static RangeBundle of(int size) {
      return new RangeBundle(size);
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      Schema schema = Schema.of(Schema.Field.of("f0", Schema.FieldType.INT64));
      Iterable<Row> bundle =
          IntStream.range(0, size)
              .mapToObj(x -> Row.withSchema(schema).addValue((long) x).build())
              .collect(Collectors.toList());

      // make sure we get one big bundle
      return input
          .getPipeline()
          .apply(Create.<Iterable<Row>>of(bundle).withCoder(IterableCoder.of(RowCoder.of(schema))))
          .apply(Flatten.iterables())
          .setRowSchema(schema);
    }
  }

  private boolean safeRun() {
    try {
      return pipeline.run().waitUntilFinish() == PipelineResult.State.DONE;
    } catch (Pipeline.PipelineExecutionException e) {
      return false;
    }
  }
}
