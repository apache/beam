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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import static java.util.stream.Collectors.joining;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables.getStackTraceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unbounded queries hand over their rows while their pipelines run. */
@RunWith(JUnit4.class)
public class LiveResultTest {
  private static final Schema SCHEMA =
      Schema.builder().addInt64Field("id").addStringField("name").build();

  /** The changes that {@link ChangesTable} emits, in order. */
  private static final List<ValueKind> KINDS =
      ImmutableList.of(
          ValueKind.INSERT,
          ValueKind.INSERT,
          ValueKind.UPDATE_BEFORE,
          ValueKind.UPDATE_AFTER,
          ValueKind.DELETE);

  private static final List<Row> ROWS =
      ImmutableList.of(row(1, "a"), row(2, "b"), row(1, "a"), row(1, "aa"), row(2, "b"));

  @After
  public void resetViewer() {
    LiveResult.setViewer(null);
  }

  @Test
  public void rowsArriveWhileThePipelineRunsUntilCancelled() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Connection connection = connect();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT id FROM sparse")) {
      // The second row comes 30 seconds after the first, and next() must not wait for it.
      long start = System.nanoTime();
      assertTrue(result.next());
      assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(20));

      // Cancel while next() waits for the second row.
      Future<?> cancel =
          executor.submit(
              () -> {
                Thread.sleep(300);
                statement.cancel();
                return null;
              });
      assertFalse(result.next());
      cancel.get();
    } finally {
      executor.shutdownNow();
    }
    assertPipelinesStopped();
  }

  @Test
  public void rowsEndWithThePipeline() throws Exception {
    List<String> rows = new ArrayList<>();
    try (Connection connection = connect();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT * FROM changes")) {
      while (result.next()) {
        rows.add(result.getLong(1) + ":" + result.getString(2));
      }
    }
    assertThat(rows, containsInAnyOrder("1:a", "2:b", "1:a", "1:aa", "2:b"));
  }

  @Test
  public void viewerSeesEachRowWithItsKind() throws Exception {
    List<String> changes = new CopyOnWriteArrayList<>();
    LiveResult.setViewer(result -> collect(result, changes, ROWS.size()));

    try (Connection connection = connect();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT id, UPPER(name) FROM changes")) {
      // The viewer showed the rows, so the query returns none.
      assertFalse(result.next());
    }
    // The kinds survive the projection.
    assertThat(
        changes,
        containsInAnyOrder(
            "INSERT:1:A", "INSERT:2:B", "UPDATE_BEFORE:1:A", "UPDATE_AFTER:1:AA", "DELETE:2:B"));
    assertPipelinesStopped();
  }

  @Test
  public void viewerReturnsWhenTheStatementIsCancelled() throws Exception {
    CountDownLatch showing = new CountDownLatch(1);
    AtomicBoolean sawCancel = new AtomicBoolean();
    LiveResult.setViewer(
        result -> {
          showing.countDown();
          sawCancel.set(waitFor(result::isCancelRequested));
        });

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Connection connection = connect();
        Statement statement = connection.createStatement()) {
      Future<ResultSet> query =
          executor.submit(() -> statement.executeQuery("SELECT * FROM sparse"));
      assertTrue(showing.await(30, TimeUnit.SECONDS));
      statement.cancel();
      // The statement ends quietly: cancelling is how the user leaves the viewer.
      query.get(30, TimeUnit.SECONDS).close();
    } finally {
      executor.shutdownNow();
    }
    assertTrue(sawCancel.get());
    assertPipelinesStopped();
  }

  @Test
  public void failedPipelineFailsTheQuery() throws Exception {
    try (Connection connection = connect();
        Statement statement = connection.createStatement()) {
      Exception rows =
          assertThrows(
              Exception.class,
              () -> {
                try (ResultSet result = statement.executeQuery("SELECT * FROM failing")) {
                  result.next();
                }
              });
      assertThat(getStackTraceAsString(rows), containsString("bad change"));

      LiveResult.setViewer(result -> waitFor(result::isFinished));
      Exception viewed =
          assertThrows(Exception.class, () -> statement.executeQuery("SELECT * FROM failing"));
      assertThat(getStackTraceAsString(viewed), containsString("bad change"));
    }
  }

  private static Row row(long id, String name) {
    return Row.withSchema(SCHEMA).addValues(id, name).build();
  }

  private static Connection connect() {
    return JdbcDriver.connect(
        new ReadOnlyTableProvider(
            "test",
            ImmutableMap.of(
                "changes", new ChangesTable(Source.ONCE),
                "sparse", new ChangesTable(Source.SPARSE),
                "failing", new ChangesTable(Source.FAILING))),
        PipelineOptionsFactory.create());
  }

  /** Takes up to {@code count} changes as "KIND:value:value", or fewer if they stop coming. */
  private static void collect(LiveResult result, List<String> changes, int count)
      throws IOException {
    try {
      while (changes.size() < count) {
        LiveResult.Change change = result.poll(1, TimeUnit.MINUTES);
        if (change == null) {
          return;
        }
        changes.add(
            change.getKind()
                + ":"
                + change.getRow().getValues().stream().map(String::valueOf).collect(joining(":")));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  /** Waits for every pipeline that a live result started to stop. */
  private static void assertPipelinesStopped() {
    assertTrue(
        "A pipeline kept running",
        waitFor(
            () ->
                Thread.getAllStackTraces().keySet().stream()
                    .noneMatch(thread -> thread.getName().equals("BeamSqlLiveResultWatcher"))));
  }

  /** Waits up to a minute for {@code condition}, and returns whether it held. */
  private static boolean waitFor(BooleanSupplier condition) {
    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        return false;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  private enum Source {
    /** Emits each change once, and then the pipeline ends. */
    ONCE,
    /** Repeats the changes forever, one every 30 seconds. */
    SPARSE,
    /** Fails on its first change. */
    FAILING
  }

  /** An unbounded table whose rows carry change kinds, like a CDC source. */
  private static class ChangesTable extends SchemaBaseBeamTable {
    private final Source source;

    ChangesTable(Source source) {
      super(SCHEMA);
      this.source = source;
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      PCollection<Long> indexes =
          source == Source.SPARSE
              ? begin.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(30)))
              : begin.apply(
                  TestStream.create(VarLongCoder.of())
                      .addElements(0L, 1L, 2L, 3L, 4L)
                      .advanceWatermarkToInfinity());
      return indexes.apply(ParDo.of(new ToChange(source == Source.FAILING))).setRowSchema(SCHEMA);
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.UNBOUNDED;
    }

    @Override
    public BeamTableStatistics getTableStatistics(PipelineOptions options) {
      return BeamTableStatistics.UNBOUNDED_UNKNOWN;
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      throw new UnsupportedOperationException("read-only");
    }
  }

  /** Turns an index into the change at that position, with its kind. */
  private static class ToChange extends DoFn<Long, Row> {
    private final boolean fail;

    ToChange(boolean fail) {
      this.fail = fail;
    }

    @ProcessElement
    public void processElement(@Element Long index, OutputReceiver<Row> out) {
      if (fail) {
        throw new IllegalStateException("bad change " + index);
      }
      int i = (int) (index % ROWS.size());
      out.builder(ROWS.get(i)).setValueKind(KINDS.get(i)).output();
    }
  }
}
