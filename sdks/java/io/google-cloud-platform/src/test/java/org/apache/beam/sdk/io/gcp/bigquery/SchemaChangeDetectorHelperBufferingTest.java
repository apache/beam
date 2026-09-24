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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.services.bigquery.model.TableRow;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link SchemaChangeDetectorHelper#bufferMismatchedRows}, the state machine that decides
 * whether a schema-mismatched row is parked in state for another retry or given up on and routed to
 * the failed-rows collection.
 *
 * <p>This method owns three pieces of persistent state at once -- the buffer itself, the retry
 * timer, and a watermark hold -- and they have to stay consistent with each other. In particular a
 * timer or a hold must never be left behind when nothing was actually buffered, since there would
 * then be no future callback that clears them.
 */
@RunWith(JUnit4.class)
public class SchemaChangeDetectorHelperBufferingTest {

  private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");
  private static final Duration RETRY_PERIOD = Duration.standardMinutes(1);
  private static final Instant ELEMENT_TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

  private FakeBagState<StoragePayloadWithDeadline> bufferedBag;
  private FakeValueState<Long> currentTimerValue;
  private FakeValueState<Long> minPendingTimestamp;
  private FakeTimer retryTimer;
  private TableDestination tableDestination;
  private Counter counter;
  private AppendClientInfo appendClientInfo;

  @SuppressWarnings("unchecked")
  private DoFn.OutputReceiver<BigQueryStorageApiInsertError> failedRowsReceiver =
      mock(DoFn.OutputReceiver.class);

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    bufferedBag = new FakeBagState<>();
    currentTimerValue = new FakeValueState<>();
    minPendingTimestamp = new FakeValueState<>();
    retryTimer = new FakeTimer(NOW);
    tableDestination = new TableDestination("project-id:dataset-id.table", null);
    counter = Metrics.counter(SchemaChangeDetectorHelperBufferingTest.class, "failedRows");
    appendClientInfo = mock(AppendClientInfo.class);
    failedRowsReceiver = mock(DoFn.OutputReceiver.class);
  }

  /** A row that is still worth retrying, i.e. whose deadline is in the future. */
  private static StoragePayloadWithDeadline liveRow(String name, Instant deadline)
      throws Exception {
    return row(name, deadline, null);
  }

  private static StoragePayloadWithDeadline row(
      String name, Instant deadline, @Nullable Instant timestamp) throws Exception {
    StorageApiWritePayload payload =
        StorageApiWritePayload.of(
            name.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            null,
            new TableRow().set("name", name));
    if (timestamp != null) {
      payload = payload.withTimestamp(timestamp);
    }
    return StoragePayloadWithDeadline.of(payload, deadline);
  }

  private void buffer(
      Iterable<StoragePayloadWithDeadline> rows, @Nullable AppendClientInfo clientInfo)
      throws Exception {
    SchemaChangeDetectorHelper.bufferMismatchedRows(
        rows,
        bufferedBag,
        retryTimer,
        currentTimerValue,
        minPendingTimestamp,
        tableDestination,
        failedRowsReceiver,
        clientInfo,
        counter,
        RETRY_PERIOD,
        ELEMENT_TIMESTAMP);
  }

  private List<BigQueryStorageApiInsertError> capturedFailedRows() {
    ArgumentCaptor<BigQueryStorageApiInsertError> captor =
        ArgumentCaptor.forClass(BigQueryStorageApiInsertError.class);
    verify(failedRowsReceiver, org.mockito.Mockito.atLeast(0))
        .outputWithTimestamp(captor.capture(), any(Instant.class));
    return captor.getAllValues();
  }

  private List<String> bufferedNames() throws Exception {
    List<String> names = Lists.newArrayList();
    for (StoragePayloadWithDeadline row : bufferedBag.read()) {
      names.add((String) row.getStoragePayload().getFailsafeTableRow().get("name"));
    }
    return names;
  }

  /**
   * The {@code processElement} path passes a null client, meaning "no deadline enforcement here".
   * Every row must be buffered even if its deadline has already lapsed, because that path has no
   * way to convert a row to a failed row yet.
   */
  @Test
  public void testNullAppendClientInfoBuffersEveryRowRegardlessOfDeadline() throws Exception {
    StoragePayloadWithDeadline expired = liveRow("expired", NOW.minus(Duration.standardHours(1)));
    StoragePayloadWithDeadline live = liveRow("live", NOW.plus(Duration.standardHours(1)));

    buffer(ImmutableList.of(expired, live), null);

    assertEquals(ImmutableList.of("expired", "live"), bufferedNames());
    verify(failedRowsReceiver, never()).outputWithTimestamp(any(), any());
  }

  /** With a client available, rows past their deadline are given up on rather than re-buffered. */
  @Test
  public void testRowsPastDeadlineAreDeadLettered() throws Exception {
    StoragePayloadWithDeadline expired = liveRow("expired", NOW.minus(Duration.standardHours(1)));
    StoragePayloadWithDeadline live = liveRow("live", NOW.plus(Duration.standardHours(1)));

    buffer(ImmutableList.of(expired, live), appendClientInfo);

    assertEquals(ImmutableList.of("live"), bufferedNames());

    List<BigQueryStorageApiInsertError> failed = capturedFailedRows();
    assertEquals(1, failed.size());
    assertEquals("expired", failed.get(0).getRow().get("name"));
    assertEquals(
        "Timed out waiting for table schema update (Mismatched schema)",
        failed.get(0).getErrorMessage());
    assertEquals(tableDestination.getTableReference(), failed.get(0).getTable());

    StoragePayloadWithDeadline expiredWithUnknown =
        StoragePayloadWithDeadline.of(
            StorageApiWritePayload.of(
                new byte[0], new TableRow().set("extra", "val"), new TableRow().set("name", "u")),
            NOW.minus(Duration.standardHours(1)));
    buffer(ImmutableList.of(expiredWithUnknown), appendClientInfo);
    failed = capturedFailedRows();
    assertEquals(2, failed.size());
    assertEquals(
        "Timed out waiting for table schema update. Unknown fields: [extra]",
        failed.get(1).getErrorMessage());
  }

  /**
   * A row is retried while its deadline is strictly in the future; a deadline exactly equal to the
   * current time is already spent. This is the boundary that decides whether a row gets one more
   * attempt or is dropped, so it is pinned explicitly.
   */
  @Test
  public void testDeadlineExactlyAtCurrentTimeIsTreatedAsExpired() throws Exception {
    buffer(ImmutableList.of(liveRow("boundary", NOW)), appendClientInfo);

    assertTrue(bufferedNames().isEmpty());
    assertEquals(1, capturedFailedRows().size());
  }

  /**
   * Regression guard: when every row is dead-lettered nothing is left in the buffer, so no retry
   * timer and no watermark hold may be left behind. A timer set here would fire against an empty
   * bag, and a hold would pin the output watermark with no data behind it.
   */
  @Test
  public void testNoTimerOrHoldWhenNothingIsBuffered() throws Exception {
    buffer(
        ImmutableList.of(
            liveRow("a", NOW.minus(Duration.standardHours(1))),
            liveRow("b", NOW.minus(Duration.standardHours(2)))),
        appendClientInfo);

    assertTrue(bufferedNames().isEmpty());
    assertNull("no retry timer may be scheduled", retryTimer.setTime);
    assertNull("no watermark hold may be taken", retryTimer.outputTimestamp);
    assertNull("timer bookkeeping must stay untouched", currentTimerValue.read());
    assertNull("watermark bookkeeping must stay untouched", minPendingTimestamp.read());
  }

  /** With nothing previously scheduled, the retry lands one retry period out. */
  @Test
  public void testTimerScheduledAtRetryPeriodWhenNonePending() throws Exception {
    buffer(ImmutableList.of(liveRow("a", NOW.plus(Duration.standardHours(1)))), appendClientInfo);

    assertEquals(NOW.plus(RETRY_PERIOD), retryTimer.setTime);
    assertEquals(Long.valueOf(NOW.plus(RETRY_PERIOD).getMillis()), currentTimerValue.read());
  }

  /**
   * An already-scheduled retry wins. Rescheduling on every arrival would let a steady trickle of
   * mismatched rows push the retry out indefinitely, so the earlier rows would never be retried.
   */
  @Test
  public void testExistingTimerIsNotPushedOutByLaterRows() throws Exception {
    Instant alreadyScheduled = NOW.plus(Duration.standardSeconds(5));
    currentTimerValue.write(alreadyScheduled.getMillis());

    buffer(ImmutableList.of(liveRow("a", NOW.plus(Duration.standardHours(1)))), appendClientInfo);

    assertEquals(alreadyScheduled, retryTimer.setTime);
    assertEquals(Long.valueOf(alreadyScheduled.getMillis()), currentTimerValue.read());
  }

  /** The watermark hold has to cover the earliest buffered row, not the most recent one. */
  @Test
  public void testHoldTracksEarliestBufferedRow() throws Exception {
    Instant early = ELEMENT_TIMESTAMP.minus(Duration.standardHours(2));
    Instant late = ELEMENT_TIMESTAMP.minus(Duration.standardMinutes(1));
    Instant deadline = NOW.plus(Duration.standardHours(1));

    buffer(
        ImmutableList.of(row("late", deadline, late), row("early", deadline, early)),
        appendClientInfo);

    assertEquals(early, retryTimer.outputTimestamp);
    assertEquals(Long.valueOf(early.getMillis()), minPendingTimestamp.read());
  }

  /** A hold already taken for an earlier row must not be released by a later one. */
  @Test
  public void testHoldIsNotReleasedByLaterRow() throws Exception {
    Instant alreadyHeld = ELEMENT_TIMESTAMP.minus(Duration.standardHours(5));
    minPendingTimestamp.write(alreadyHeld.getMillis());

    buffer(
        ImmutableList.of(
            row(
                "later",
                NOW.plus(Duration.standardHours(1)),
                ELEMENT_TIMESTAMP.minus(Duration.standardMinutes(1)))),
        appendClientInfo);

    assertEquals(alreadyHeld, retryTimer.outputTimestamp);
    assertEquals(Long.valueOf(alreadyHeld.getMillis()), minPendingTimestamp.read());
  }

  /** Rows carrying no timestamp of their own fall back to the enclosing element's timestamp. */
  @Test
  public void testFallsBackToElementTimestampWhenRowHasNone() throws Exception {
    buffer(ImmutableList.of(liveRow("a", NOW.plus(Duration.standardHours(1)))), appendClientInfo);

    assertEquals(ELEMENT_TIMESTAMP, retryTimer.outputTimestamp);
    assertEquals(Long.valueOf(ELEMENT_TIMESTAMP.getMillis()), minPendingTimestamp.read());
  }

  /** Buffering appends to whatever is already parked rather than replacing it. */
  @Test
  public void testBufferingAccumulatesAcrossCalls() throws Exception {
    Instant deadline = NOW.plus(Duration.standardHours(1));

    buffer(ImmutableList.of(liveRow("first", deadline)), appendClientInfo);
    buffer(ImmutableList.of(liveRow("second", deadline)), appendClientInfo);

    assertEquals(ImmutableList.of("first", "second"), bufferedNames());
  }

  @Test
  public void testDeadLetteredRowsAreCountedAndTimestamped() throws Exception {
    Instant rowTimestamp = ELEMENT_TIMESTAMP.minus(Duration.standardMinutes(7));
    buffer(
        ImmutableList.of(row("expired", NOW.minus(Duration.standardHours(1)), rowTimestamp)),
        appendClientInfo);

    verify(failedRowsReceiver, times(1))
        .outputWithTimestamp(any(), org.mockito.ArgumentMatchers.eq(rowTimestamp));
  }

  // ---------------------------------------------------------------------------------------------
  // Minimal in-memory state and timer doubles. These deliberately record what was written rather
  // than emulating a runner, so that assertions can distinguish "never set" from "set to a value".
  // ---------------------------------------------------------------------------------------------

  private static class FakeBagState<T> implements BagState<T> {
    private final List<T> contents = Lists.newArrayList();

    @Override
    public void add(T value) {
      contents.add(value);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          return contents.isEmpty();
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public Iterable<T> read() {
      return ImmutableList.copyOf(contents);
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }

    @Override
    public void clear() {
      contents.clear();
    }
  }

  private static class FakeValueState<T> implements ValueState<T> {
    private @Nullable T value = null;

    @Override
    public void write(T input) {
      this.value = input;
    }

    @Override
    public @Nullable T read() {
      return value;
    }

    @Override
    public ValueState<T> readLater() {
      return this;
    }

    @Override
    public void clear() {
      value = null;
    }
  }

  private static class FakeTimer implements Timer {
    private final Instant currentRelativeTime;

    private @Nullable Instant setTime = null;
    private @Nullable Instant outputTimestamp = null;

    FakeTimer(Instant currentRelativeTime) {
      this.currentRelativeTime = currentRelativeTime;
    }

    @Override
    public void set(Instant absoluteTime) {
      this.setTime = absoluteTime;
    }

    @Override
    public void setRelative() {
      this.setTime = currentRelativeTime;
    }

    @Override
    public void clear() {
      this.setTime = null;
    }

    @Override
    public Timer offset(Duration offset) {
      return this;
    }

    @Override
    public Timer align(Duration period) {
      return this;
    }

    @Override
    public Timer withOutputTimestamp(Instant outputTime) {
      this.outputTimestamp = outputTime;
      return this;
    }

    @Override
    public Timer withNoOutputTimestamp() {
      this.outputTimestamp = null;
      return this;
    }

    @Override
    public Instant getCurrentRelativeTime() {
      return currentRelativeTime;
    }
  }
}
