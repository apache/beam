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
package org.apache.beam.sdk.fn.data;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnDataInboundObserver}. */
@RunWith(JUnit4.class)
public class BeamFnDataInboundObserverTest {
  private static final Coder<WindowedValue<String>> CODER =
      WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
  private static final String TRANSFORM_ID = "transformId";
  private static final String TIMER_FAMILY_ID = "timerFamilyId";

  @Rule
  public final TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);

  @Test
  public void testConsumptionOfValuesHappensOnAwaitCompletionCallersThread() throws Exception {
    Thread thread = Thread.currentThread();
    Collection<WindowedValue<String>> values = new ArrayList<>();
    Collection<WindowedValue<String>> timers = new ArrayList<>();
    BeamFnDataInboundObserver observer =
        BeamFnDataInboundObserver.forConsumers(
            Arrays.asList(
                DataEndpoint.create(
                    TRANSFORM_ID,
                    CODER,
                    (value) -> {
                      assertSame(thread, Thread.currentThread());
                      values.add(value);
                    })),
            Arrays.asList(
                TimerEndpoint.create(
                    TRANSFORM_ID,
                    TIMER_FAMILY_ID,
                    CODER,
                    (value) -> {
                      assertSame(thread, Thread.currentThread());
                      timers.add(value);
                    })));

    Future<?> future =
        executor.submit(
            () -> {
              // Test decoding multiple messages
              observer.accept(dataWith("ABC", "DEF", "GHI"));
              observer.accept(lastData());
              observer.accept(timerWith("UVW"));
              observer.accept(timerWith("XYZ"));
              observer.accept(lastTimer());
              return null;
            });

    observer.awaitCompletion();
    assertThat(
        values,
        contains(
            WindowedValue.valueInGlobalWindow("ABC"),
            WindowedValue.valueInGlobalWindow("DEF"),
            WindowedValue.valueInGlobalWindow("GHI")));
    assertThat(
        timers,
        contains(
            WindowedValue.valueInGlobalWindow("UVW"), WindowedValue.valueInGlobalWindow("XYZ")));
    future.get();
  }

  @Test
  public void testAwaitCompletionFailureVisibleToAwaitCompletionCallerAndProducer()
      throws Exception {
    BeamFnDataInboundObserver observer =
        BeamFnDataInboundObserver.forConsumers(
            Arrays.asList(
                DataEndpoint.create(
                    TRANSFORM_ID,
                    CODER,
                    (value) -> {
                      throw new Exception("test consumer failed");
                    })),
            Collections.emptyList());

    Future<?> future =
        executor.submit(
            () -> {
              observer.accept(dataWith("ABC"));
              assertThrows(
                  "test consumer failed",
                  Exception.class,
                  () -> {
                    while (true) {
                      // keep trying to send messages since the queue buffers messages and the
                      // consumer
                      // may have not yet noticed the bad state.
                      observer.accept(dataWith("ABC"));
                    }
                  });
              return null;
            });

    assertThrows("test consumer failed", Exception.class, () -> observer.awaitCompletion());
    future.get();
  }

  @Test
  public void testCloseVisibleToAwaitCompletionCallerAndProducer() throws Exception {
    BeamFnDataInboundObserver observer =
        BeamFnDataInboundObserver.forConsumers(
            Arrays.asList(DataEndpoint.create(TRANSFORM_ID, CODER, (value) -> {})),
            Collections.emptyList());

    AtomicBoolean isReady = new AtomicBoolean(false);
    Future<?> future =
        executor.submit(
            () -> {
              observer.accept(dataWith("ABC"));
              synchronized (isReady) {
                isReady.set(true);
                isReady.notify();
              }
              assertThrows(
                  BeamFnDataInboundObserver.CloseException.class,
                  () -> {
                    while (true) {
                      // keep trying to send messages since the queue buffers messages and the
                      // consumer
                      // may have not yet noticed the bad state.
                      observer.accept(dataWith("ABC"));
                    }
                  });
              return null;
            });
    Future<?> future2 =
        executor.submit(
            () -> {
              synchronized (isReady) {
                while (!isReady.get()) {
                  isReady.wait();
                }
              }
              observer.close();
              return null;
            });

    assertThrows(BeamFnDataInboundObserver.CloseException.class, () -> observer.awaitCompletion());
    future.get();
    future2.get();
  }

  @Test
  public void testBadProducerDataFailureVisibleToAwaitCompletionCallerAndProducer()
      throws Exception {
    BeamFnDataInboundObserver observer =
        BeamFnDataInboundObserver.forConsumers(
            Arrays.asList(DataEndpoint.create(TRANSFORM_ID, CODER, (value) -> {})),
            Collections.emptyList());
    Future<?> future =
        executor.submit(
            () -> {
              observer.accept(timerWith("DEF"));
              assertThrows(
                  "Unable to find inbound timer receiver for instruction",
                  IllegalStateException.class,
                  () -> {
                    // keep trying to send messages since the queue buffers messages and the
                    // consumer
                    // may have not yet noticed the bad state.
                    while (true) {
                      observer.accept(dataWith("ABC"));
                    }
                  });
              return null;
            });

    assertThrows(
        "Unable to find inbound timer receiver for instruction",
        IllegalStateException.class,
        () -> observer.awaitCompletion());
    future.get();
  }

  private BeamFnApi.Elements dataWith(String... values) throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (String value : values) {
      CODER.encode(WindowedValue.valueInGlobalWindow(value), output);
    }
    return BeamFnApi.Elements.newBuilder()
        .addData(
            BeamFnApi.Elements.Data.newBuilder()
                .setTransformId(TRANSFORM_ID)
                .setData(output.toByteString()))
        .build();
  }

  private BeamFnApi.Elements lastData() throws Exception {
    return BeamFnApi.Elements.newBuilder()
        .addData(BeamFnApi.Elements.Data.newBuilder().setTransformId(TRANSFORM_ID).setIsLast(true))
        .build();
  }

  private BeamFnApi.Elements timerWith(String... values) throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    for (String value : values) {
      CODER.encode(WindowedValue.valueInGlobalWindow(value), output);
    }
    return BeamFnApi.Elements.newBuilder()
        .addTimers(
            BeamFnApi.Elements.Timers.newBuilder()
                .setTransformId(TRANSFORM_ID)
                .setTimerFamilyId(TIMER_FAMILY_ID)
                .setTimers(output.toByteString()))
        .build();
  }

  private BeamFnApi.Elements lastTimer() throws Exception {
    return BeamFnApi.Elements.newBuilder()
        .addTimers(
            BeamFnApi.Elements.Timers.newBuilder()
                .setTransformId(TRANSFORM_ID)
                .setTimerFamilyId(TIMER_FAMILY_ID)
                .setIsLast(true))
        .build();
  }
}
