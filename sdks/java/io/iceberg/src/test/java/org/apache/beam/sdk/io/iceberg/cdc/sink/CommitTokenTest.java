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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CommitToken}. */
@RunWith(JUnit4.class)
public class CommitTokenTest {

  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();

  private static final String SINK_ID = "sink-a";
  private static final String RUN_ID = "run-1";
  private static final String DEST = "db.t";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private Table table;
  private CommitToken token;

  @Before
  public void setUp() {
    Catalog catalog = CdcSinkTestUtils.hadoopCatalog(tmp.getRoot());
    table =
        CdcSinkTestUtils.createTable(
            catalog,
            TableIdentifier.of("db", "t" + System.nanoTime()),
            SCHEMA,
            ImmutableSet.of(1),
            2,
            PartitionSpec.unpartitioned());
    token =
        new CommitToken(
            SINK_ID,
            RUN_ID,
            Metrics.counter(CommitTokenTest.class, "tokenParseFailures"),
            Metrics.counter(CommitTokenTest.class, "suspectedTokenExpiry"));
  }

  /** Commits an empty snapshot carrying a full token. */
  private void commitToken(long committedThroughMs, long maxSeq, @Nullable Integer specId) {
    AppendFiles op = table.newAppend();
    token.writeTo(op, committedThroughMs, maxSeq, specId);
    op.commit();
  }

  private Map<String, String> currentSummary() {
    return checkStateNotNull(table.currentSnapshot()).summary();
  }

  @Test
  public void emptyTableIsAFreshStart() {
    CommitToken.Recovered recovered = token.recoverFrom(table, DEST);
    assertThat(recovered.committedThroughMs, equalTo(Long.MIN_VALUE));
    assertThat(recovered.maxCommittedSeq, equalTo(Long.MIN_VALUE));
    assertThat(CommitToken.readRunSpec(table, SINK_ID, RUN_ID), nullValue());
  }

  @Test
  public void newestTokenIsRecoveredWithItsOwnMaxSeq() {
    commitToken(1_000L, 5L, 3);
    commitToken(2_000L, 9L, 3);

    CommitToken.Recovered recovered = token.recoverFrom(table, DEST);
    assertThat(recovered.committedThroughMs, equalTo(2_000L));
    assertThat(recovered.maxCommittedSeq, equalTo(9L));
    assertThat(CommitToken.readRunSpec(table, SINK_ID, RUN_ID), equalTo(3));
    assertThat(currentSummary().get(CommitToken.SINK_ID_KEY), equalTo(SINK_ID));
  }

  @Test
  public void unparseableTokenIsSkippedWithoutLendingItsMaxSeq() {
    commitToken(1_000L, 5L, null);
    AppendFiles corrupt = table.newAppend();
    corrupt.set(CommitToken.COMMITTED_THROUGH_MS_PREFIX + SINK_ID, "nope");
    corrupt.set(CommitToken.MAX_COMMITTED_SEQ_PREFIX + SINK_ID, "99");
    corrupt.commit();

    CommitToken.Recovered recovered = token.recoverFrom(table, DEST);
    assertThat(recovered.committedThroughMs, equalTo(1_000L));
    assertThat(recovered.maxCommittedSeq, equalTo(5L));
  }

  @Test
  public void otherSinksAndRunsAreIgnored() {
    table
        .newAppend()
        .set(CommitToken.SINK_ID_KEY, "sink-b")
        .set(CommitToken.COMMITTED_THROUGH_MS_PREFIX + "sink-b", "7")
        .set(CommitToken.RUN_SPEC_PREFIX + SINK_ID, "run-2:4")
        .commit();

    assertThat(token.recoverFrom(table, DEST).committedThroughMs, equalTo(Long.MIN_VALUE));
    assertThat(CommitToken.readRunSpec(table, SINK_ID, RUN_ID), nullValue());
    assertThat(CommitToken.readRunSpec(table, SINK_ID, "run-2"), equalTo(4));
  }

  @Test
  public void heartbeatOmitsAnUnknownMaxSeq() {
    AppendFiles op = table.newAppend();
    token.writeHeartbeatTo(op, 3_000L, Long.MIN_VALUE, 2);
    op.commit();

    Map<String, String> summary = currentSummary();
    assertThat(summary.get(CommitToken.COMMITTED_THROUGH_MS_PREFIX + SINK_ID), equalTo("3000"));
    assertThat(summary.containsKey(CommitToken.MAX_COMMITTED_SEQ_PREFIX + SINK_ID), equalTo(false));
    assertThat(summary.get(CommitToken.RUN_SPEC_PREFIX + SINK_ID), equalTo("run-1:2"));
    assertThat(token.recoverFrom(table, DEST).maxCommittedSeq, equalTo(Long.MIN_VALUE));
  }

  @Test
  public void heartbeatIsDueOnceTheTokenSnapshotIsOlderThanTheInterval() {
    assertThat(token.shouldHeartbeat(table, 1_000L, 0L), equalTo(false));
    commitToken(1_000L, 1L, null);
    long committedAt = checkStateNotNull(table.currentSnapshot()).timestampMillis();

    assertThat(token.shouldHeartbeat(table, 1_000L, committedAt + 500L), equalTo(false));
    assertThat(token.shouldHeartbeat(table, 1_000L, committedAt + 1_001L), equalTo(true));
  }

  @Test
  public void findsTheSnapshotCarryingAWindowToken() {
    commitToken(1_000L, 1L, null);
    Snapshot first = checkStateNotNull(table.currentSnapshot());
    commitToken(2_000L, 2L, null);

    assertThat(
        token.findRecentlyCommittedTokenSnapshot(table, DEST, 1_000L).snapshotId(),
        equalTo(first.snapshotId()));
    assertThrows(
        IllegalStateException.class,
        () -> token.findRecentlyCommittedTokenSnapshot(table, DEST, 3_000L));
  }
}
