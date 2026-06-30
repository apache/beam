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
package org.apache.beam.sdk.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for verifying async processing structures and logic. */
@RunWith(JUnit4.class)
public class AsyncWrapperTest implements Serializable {

  private final boolean useThreadPool = true;

  // Used for testing basic DoFn processing logic with optional latency.
  private static class BasicDofn extends DoFn<String, String> {
    private final long sleepTimeMs;
    private int processed = 0;
    private final ReentrantLock lock = new ReentrantLock();

    BasicDofn(long sleepTimeMs) {
      this.sleepTimeMs = sleepTimeMs;
    }

    BasicDofn() {
      this(0);
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      if (sleepTimeMs > 0) {
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      lock.lock();
      try {
        processed += 1;
      } finally {
        lock.unlock();
      }
      receiver.output(element);
    }

    int getProcessed() {
      lock.lock();
      try {
        return processed;
      } finally {
        lock.unlock();
      }
    }
  }

  // Used for testing multi element processing with optional finish bundle call.
  private static class MultiElementDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      receiver.output(element);
      receiver.output(element);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      c.output("bundle end", Instant.now(), GlobalWindow.INSTANCE);
    }
  }

  // Used for testing BagState thread safety.
  private static class FakeBagState<T> implements BagState<T> {
    private final List<T> items;
    private final ReentrantLock lock = new ReentrantLock();

    FakeBagState(List<T> initialItems) {
      this.items = new ArrayList<>(initialItems);
    }

    FakeBagState(T initialItem) {
      this(new ArrayList<>(List.of(initialItem)));
    }

    FakeBagState() {
      this(new ArrayList<>());
    }

    @Override
    public void add(T item) {
      lock.lock();
      try {
        items.add(item);
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void clear() {
      lock.lock();
      try {
        items.clear();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public Iterable<T> read() {
      lock.lock();
      try {
        return new ArrayList<>(items);
      } finally {
        lock.unlock();
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          lock.lock();
          try {
            return items.isEmpty();
          } finally {
            lock.unlock();
          }
        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }
  }

  // 4. Used for testing Timer mock implementations.
  private static class FakeTimer implements Timer {
    private Instant time = Instant.EPOCH;

    @Override
    public void set(Instant absoluteTime) {
      this.time = absoluteTime;
    }

    @Override
    public void setRelative() {}

    @Override
    public void clear() {
      this.time = Instant.EPOCH;
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
      return this;
    }

    @Override
    public Timer withNoOutputTimestamp() {
      return this;
    }

    @Override
    public Instant getCurrentRelativeTime() {
      return time;
    }
  }

  @Before
  public void setUp() {
    AsyncWrapper.resetState();
  }

  private void waitForEmpty(AsyncWrapper<?, ?, ?> asyncWrapper) {
    waitForEmpty(asyncWrapper, 10);
  }

  private void waitForEmpty(AsyncWrapper<?, ?, ?> asyncWrapper, int timeoutSeconds) {
    long limit = System.currentTimeMillis() + timeoutSeconds * 1000L;
    while (!asyncWrapper.isEmpty()) {
      if (System.currentTimeMillis() > limit) {
        throw new RuntimeException("Timed out waiting for async dofn to be empty");
      }
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    try {
      Thread.sleep(5);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private <T> void checkOutput(List<T> result, List<T> expectedOutput) {
    List<String> resultStr = new ArrayList<>();
    for (T val : result) {
      resultStr.add(val.toString());
    }
    List<String> expectedStr = new ArrayList<>();
    for (T val : expectedOutput) {
      expectedStr.add(val.toString());
    }
    Collections.sort(resultStr);
    Collections.sort(expectedStr);
    assertEquals(expectedStr, resultStr);
  }

  private void checkItemsInBuffer(AsyncWrapper<?, ?, ?> asyncWrapper, int expectedCount) {
    assertEquals(expectedCount, asyncWrapper.getItemsInBufferCount());
  }

  // Test 1: testCustomIdFn
  // Verifies custom ID extraction and deduplication of in-flight duplicate elements.
  @Test
  public void testCustomIdFn() {
    class CustomIdObject implements Serializable {
      final int elementId;
      final String value;

      CustomIdObject(int elementId, String value) {
        this.elementId = elementId;
        this.value = value;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof CustomIdObject)) {
          return false;
        }
        CustomIdObject that = (CustomIdObject) o;
        return elementId == that.elementId;
      }

      @Override
      public int hashCode() {
        return java.util.Objects.hash(elementId);
      }

      @Override
      public String toString() {
        return "CustomIdObject{id=" + elementId + ", val=" + value + "}";
      }
    }

    class CustomIdDofn extends DoFn<CustomIdObject, String> {
      @ProcessElement
      public void processElement(@Element CustomIdObject element, OutputReceiver<String> receiver) {
        receiver.output(element.value);
      }
    }

    CustomIdDofn dofn = new CustomIdDofn();
    AsyncWrapper<String, CustomIdObject, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn,
            1,
            Duration.standardSeconds(5),
            null,
            null,
            null,
            x -> x.elementId,
            useThreadPool);
    asyncWrapper.setup(null);

    FakeBagState<KV<String, CustomIdObject>> fakeBagState = new FakeBagState<>();
    FakeTimer fakeTimer = new FakeTimer();

    KV<String, CustomIdObject> msg1 = KV.of("key1", new CustomIdObject(1, "a"));
    KV<String, CustomIdObject> msg2 = KV.of("key1", new CustomIdObject(1, "b"));

    asyncWrapper.processDirect(msg1, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);
    asyncWrapper.processDirect(msg2, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);

    waitForEmpty(asyncWrapper);

    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.singletonList("a"));
    assertEquals(0, fakeBagState.items.size());
  }

  // Test 2: testBasic
  // Verifies the standard end-to-end execution flow. Elements should be queued in persistent state
  // and output correctly upon completion.
  @Test
  public void testBasic() {
    BasicDofn dofn = new BasicDofn();
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();
    FakeTimer fakeTimer = new FakeTimer();
    KV<String, String> msg = KV.of("key1", "1");

    asyncWrapper.processDirect(msg, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);

    assertEquals(1, fakeBagState.items.size());
    assertNotEquals(Instant.EPOCH, fakeTimer.getCurrentRelativeTime());

    waitForEmpty(asyncWrapper);

    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.singletonList("1"));
    assertEquals(1, dofn.getProcessed());
    assertEquals(0, fakeBagState.items.size());
  }

  // Test 3: testMultiKey
  // Verifies key grouping isolation. Firing a timer for one partition key must not release
  // or interfere with elements queued under a different partition key.
  @Test
  public void testMultiKey() {
    for (boolean useThreadPool : new boolean[] {true, false}) {
      BasicDofn dofn = new BasicDofn();
      AsyncWrapper<String, String, String> asyncWrapper =
          new AsyncWrapper<>(
              dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
      asyncWrapper.setup(null);

      FakeBagState<KV<String, String>> fakeBagStateKey1 = new FakeBagState<>();
      FakeBagState<KV<String, String>> fakeBagStateKey2 = new FakeBagState<>();
      FakeTimer fakeTimer = new FakeTimer();

      KV<String, String> msg1 = KV.of("key1", "1");
      KV<String, String> msg2 = KV.of("key2", "2");

      asyncWrapper.processDirect(
          msg1, GlobalWindow.INSTANCE, Instant.now(), fakeBagStateKey1, fakeTimer);
      asyncWrapper.processDirect(
          msg2, GlobalWindow.INSTANCE, Instant.now(), fakeBagStateKey2, fakeTimer);

      waitForEmpty(asyncWrapper);

      List<String> result =
          asyncWrapper.commitFinishedItemsDirect(
              fakeTimer.getCurrentRelativeTime(), fakeBagStateKey2, fakeTimer);
      checkOutput(result, Collections.singletonList("2"));
      assertEquals(1, fakeBagStateKey1.items.size());
      assertEquals(0, fakeBagStateKey2.items.size());

      result =
          asyncWrapper.commitFinishedItemsDirect(
              fakeTimer.getCurrentRelativeTime(), fakeBagStateKey1, fakeTimer);
      checkOutput(result, Collections.singletonList("1"));
      assertEquals(0, fakeBagStateKey1.items.size());
      assertEquals(0, fakeBagStateKey2.items.size());
    }
  }

  // Test 4: testLongItem
  // Verifies that outputs are kept in-flight and not committed prematurely if the background
  // execution task has not finished processing yet.
  @Test
  public void testLongItem() {
    BasicDofn dofn = new BasicDofn(500);
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();
    FakeTimer fakeTimer = new FakeTimer();
    KV<String, String> msg = KV.of("key1", "1");

    asyncWrapper.processDirect(msg, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);

    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.emptyList());
    assertEquals(0, dofn.getProcessed());
    assertEquals(1, fakeBagState.items.size());

    waitForEmpty(asyncWrapper, 2);

    result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.singletonList("1"));
    assertEquals(1, dofn.getProcessed());
    assertEquals(0, fakeBagState.items.size());
  }

  // Test 5: testLostItem
  // Verifies if the local worker's in-memory cache is empty but the runner's
  // persistent state contains pending items.
  // The wrapper must automatically detect the mismatch and reschedule execution.
  @Test
  public void testLostItem() {
    BasicDofn dofn = new BasicDofn();
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    FakeTimer fakeTimer = new FakeTimer();
    KV<String, String> msg = KV.of("key1", "1");
    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>(msg);

    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.emptyList());

    waitForEmpty(asyncWrapper);

    result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.singletonList("1"));
  }

  // Test 6: testCancelledItem
  // Verifies active task cancellation if a pending element is deleted from the runner's state.
  @Test
  public void testCancelledItem() {
    BasicDofn dofn = new BasicDofn();
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    KV<String, String> msg1 = KV.of("key1", "1");
    KV<String, String> msg2 = KV.of("key1", "2");
    FakeTimer fakeTimer = new FakeTimer();
    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();

    asyncWrapper.processDirect(msg1, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);
    asyncWrapper.processDirect(msg2, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);

    waitForEmpty(asyncWrapper);

    fakeBagState.clear();
    fakeBagState.add(msg2);

    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.singletonList("2"));
    assertEquals(0, fakeBagState.items.size());
  }

  // Test 7: testMultiElementDofn
  // Verifies support for DoFns that emit multiple outputs per element, and correctly aggregates
  // outputs produced during the finishBundle stage of the sync DoFn's lifecycle.
  @Test
  public void testMultiElementDofn() {
    MultiElementDoFn dofn = new MultiElementDoFn();
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();
    FakeTimer fakeTimer = new FakeTimer();
    KV<String, String> msg = KV.of("key1", "1");

    asyncWrapper.processDirect(msg, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);

    waitForEmpty(asyncWrapper);

    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Arrays.asList("1", "1", "bundle end"));
    assertEquals(0, fakeBagState.items.size());
  }

  // Test 8: testDuplicates
  // Verifies deduplication of duplicate elements under active processing.
  // Identical elements should not spawn multiple concurrent background executions.
  @Test
  public void testDuplicates() {
    BasicDofn dofn = new BasicDofn(10);
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();
    FakeTimer fakeTimer = new FakeTimer();
    KV<String, String> msg = KV.of("key1", "1");

    asyncWrapper.processDirect(msg, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);
    fakeBagState.clear();
    asyncWrapper.processDirect(msg, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);

    assertEquals(1, fakeBagState.items.size());

    waitForEmpty(asyncWrapper);

    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.singletonList("1"));
    assertEquals(0, fakeBagState.items.size());
  }

  // Test 9: testSlowDuplicates
  // Verifies that duplicate elements sent after the in-memory buffer
  // has cleared are correctly tracked and processed.
  @Test
  public void testSlowDuplicates() {
    BasicDofn dofn = new BasicDofn(20);
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();
    FakeTimer fakeTimer = new FakeTimer();
    KV<String, String> msg = KV.of("key1", "1");

    asyncWrapper.processDirect(msg, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    fakeBagState.clear();
    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.emptyList());
    assertEquals(0, fakeBagState.items.size());

    asyncWrapper.processDirect(msg, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);
    assertEquals(1, fakeBagState.items.size());
    waitForEmpty(asyncWrapper);

    result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.singletonList("1"));
    assertEquals(0, fakeBagState.items.size());
  }

  // Test 10: testBufferCount
  // Verifies accurate in-flight metrics tracking.
  // The item count in the buffer must increment on task scheduling
  // and decrement immediately upon execution completion.
  @Test
  public void testBufferCount() {
    BasicDofn dofn = new BasicDofn(10);
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    KV<String, String> msg = KV.of("key1", "1");
    FakeTimer fakeTimer = new FakeTimer();
    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();

    asyncWrapper.processDirect(msg, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);
    checkItemsInBuffer(asyncWrapper, 1);

    waitForEmpty(asyncWrapper);
    checkItemsInBuffer(asyncWrapper, 0);

    asyncWrapper.commitFinishedItemsDirect(
        fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkItemsInBuffer(asyncWrapper, 0);
  }

  // Test 11: testBufferStopsAcceptingItems
  // Verifies queue boundaries and backpressure throttling.
  // When concurrent threads push elements exceeding the capacity limit,
  // the scheduler must block and delay submissions appropriately.
  @Test
  public void testBufferStopsAcceptingItems() {
    BasicDofn dofn = new BasicDofn(500);
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn,
            1,
            Duration.standardSeconds(5),
            5, // max buffer capacity
            null,
            null,
            null,
            useThreadPool);
    asyncWrapper.setup(null);

    FakeTimer fakeTimer = new FakeTimer();
    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();

    ExecutorService poolExecutor = Executors.newFixedThreadPool(10);
    List<String> expectedOutput = new ArrayList<>();
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      final int idx = i;
      expectedOutput.add(String.valueOf(idx));
      futures.add(
          poolExecutor.submit(
              () -> {
                KV<String, String> item = KV.of("key", String.valueOf(idx));
                asyncWrapper.processDirect(
                    item, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);
              }));
    }

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    assertEquals(5, asyncWrapper.getItemsInBufferCount());

    waitForEmpty(asyncWrapper, 100);

    // Verify that all background tasks completed successfully without throwing exceptions
    for (Future<?> future : futures) {
      try {
        future.get(); // This will re-throw any exception that occurred in the background thread
      } catch (Exception e) {
        throw new AssertionError("Background task failed", e);
      }
    }

    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);

    waitForEmpty(asyncWrapper, 100);

    result.addAll(
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer));

    checkOutput(result, expectedOutput);
    checkItemsInBuffer(asyncWrapper, 0);
    poolExecutor.shutdown();
  }

  // Test 12: testBufferWithCancellation
  // Verifies actively cancelled elements are cleanly dropped from the buffer during throttling.
  @Test
  public void testBufferWithCancellation() {
    BasicDofn dofn = new BasicDofn(10);
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    KV<String, String> msg1 = KV.of("key1", "1");
    KV<String, String> msg2 = KV.of("key1", "2");
    FakeTimer fakeTimer = new FakeTimer();
    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();

    asyncWrapper.processDirect(msg1, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);
    asyncWrapper.processDirect(msg2, GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);

    checkItemsInBuffer(asyncWrapper, 2);

    fakeBagState.clear();
    fakeBagState.add(msg2);

    List<String> result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkOutput(result, Collections.emptyList());
    assertEquals(1, fakeBagState.items.size());

    waitForEmpty(asyncWrapper);

    result =
        asyncWrapper.commitFinishedItemsDirect(
            fakeTimer.getCurrentRelativeTime(), fakeBagState, fakeTimer);
    checkItemsInBuffer(asyncWrapper, 0);
    checkOutput(result, Collections.singletonList("2"));
  }

  // Test 13: testLoadCorrectness
  // Verifies that the async wrapper processes large concurrent volumes
  // across multiple keys correctly under heavy multi-threaded load.
  @Test
  public void testLoadCorrectness() {
    BasicDofn dofn = new BasicDofn(10);
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn,
            1,
            Duration.standardSeconds(5),
            null,
            null,
            Duration.millis(10),
            null,
            useThreadPool);
    asyncWrapper.setup(null);

    java.util.Map<String, FakeBagState<KV<String, String>>> bagStates = new java.util.HashMap<>();
    java.util.Map<String, FakeTimer> timers = new java.util.HashMap<>();
    java.util.Map<String, List<String>> expectedOutputs = new java.util.HashMap<>();

    for (int i = 0; i < 10; i++) {
      String key = "key" + i;
      bagStates.put(key, new FakeBagState<>());
      timers.put(key, new FakeTimer());
      expectedOutputs.put(key, new ArrayList<>());
    }

    ExecutorService poolExecutor = Executors.newFixedThreadPool(10);
    List<Future<?>> futures = new ArrayList<>();
    Random random = new Random();

    for (int i = 0; i < 100; i++) {
      final int val = i;
      final String key = "key" + random.nextInt(10);
      expectedOutputs.get(key).add(String.valueOf(val));

      futures.add(
          poolExecutor.submit(
              () -> {
                KV<String, String> item = KV.of(key, String.valueOf(val));
                asyncWrapper.processDirect(
                    item,
                    GlobalWindow.INSTANCE,
                    Instant.now(),
                    bagStates.get(key),
                    timers.get(key));
              }));
      try {
        Thread.sleep(random.nextInt(2));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    try {
      Thread.sleep(1000 + random.nextInt(1000));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Verify that all background tasks completed successfully
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        throw new AssertionError("Background task failed", e);
      }
    }

    boolean done = false;
    java.util.Map<String, List<String>> results = new java.util.HashMap<>();
    for (int i = 0; i < 10; i++) {
      results.put("key" + i, new ArrayList<>());
    }

    while (!done) {
      done = true;
      for (int i = 0; i < 10; i++) {
        String key = "key" + i;
        results
            .get(key)
            .addAll(
                asyncWrapper.commitFinishedItemsDirect(
                    timers.get(key).getCurrentRelativeTime(), bagStates.get(key), timers.get(key)));
        if (!bagStates.get(key).items.isEmpty()) {
          done = false;
        } else {
          checkOutput(results.get(key), expectedOutputs.get(key));
        }
      }
      try {
        Thread.sleep(10 + random.nextInt(20));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    for (int i = 0; i < 10; i++) {
      String key = "key" + i;
      checkOutput(results.get(key), expectedOutputs.get(key));
      assertEquals(0, bagStates.get(key).items.size());
    }
    poolExecutor.shutdown();
  }

  // Test 14: testResetStateConcurrentTeardown
  // Verifies safe resource cleanup during concurrent shutdown.
  // Resetting the global shared execution state while workers are running
  // must complete cleanly without thread or lock deadlocks.
  @Test
  public void testResetStateConcurrentTeardown() {
    BasicDofn dofn = new BasicDofn(10);
    AsyncWrapper<String, String, String> asyncWrapper =
        new AsyncWrapper<>(
            dofn, 1, Duration.standardSeconds(5), null, null, null, null, useThreadPool);
    asyncWrapper.setup(null);

    FakeBagState<KV<String, String>> fakeBagState = new FakeBagState<>();
    FakeTimer fakeTimer = new FakeTimer();

    asyncWrapper.processDirect(
        KV.of("key1", "1"), GlobalWindow.INSTANCE, Instant.now(), fakeBagState, fakeTimer);

    try {
      Thread.sleep(2);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Verify calling resetState() while background tasks are running finishes cleanly
    AsyncWrapper.resetState();
  }
}
