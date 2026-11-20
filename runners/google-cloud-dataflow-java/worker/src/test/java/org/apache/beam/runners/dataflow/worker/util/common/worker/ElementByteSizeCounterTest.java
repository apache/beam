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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ElementByteSizeCounter}. */
@RunWith(JUnit4.class)
public class ElementByteSizeCounterTest {

  private static ElementByteSizeCounter makeCounter(AtomicLong sink, int samplingPeriod, int seed) {
    return new ElementByteSizeCounter(
        new ElementByteSizeObservableCoder<>(StringUtf8Coder.of()),
        sink::addAndGet,
        samplingPeriod) {
      private final Random random = new Random(seed);

      @Override
      protected Random getRandom() {
        return random;
      }
    };
  }

  @Test
  public void testSimpleByteCount() throws Exception {
    AtomicLong totalBytes = new AtomicLong();
    ElementByteSizeCounter counter = makeCounter(totalBytes, 1, 0);
    for (int i = 0; i < 10000; i++) {
      counter.update("foo");
      counter.finishLazyUpdate("foo");
    }
    assertEquals(40000L, totalBytes.get());
  }

  @Test
  public void testSamplingByteCountFewElements() throws Exception {
    AtomicLong totalBytes = new AtomicLong();
    ElementByteSizeCounter counter = makeCounter(totalBytes, 100, 0);
    for (int i = 0; i < 10; i++) {
      counter.update("foo");
      counter.finishLazyUpdate("foo");
    }
    assertEquals(40L, totalBytes.get());
  }

  @Test
  public void testSamplingByteCount() throws Exception {
    // StringUtf8Coder encodes "foo" in 4 bytes (1-byte length prefix + 3 UTF-8 bytes), so 1,000,000
    // elements have a true encoded size of 4,000,000 bytes. With samplingPeriod=100, these are the
    // deterministic sampled estimates produced by Random(seed) for seeds 0, 1, and 2 (matching
    // OutputObjectAndByteCounterTest#testSamplingByteCount).
    List<Long> expected = Arrays.asList(4009645L, 3979997L, 4014060L);
    for (int n = 0; n < 3; n++) {
      AtomicLong totalBytes = new AtomicLong();
      ElementByteSizeCounter counter = makeCounter(totalBytes, 100, n);
      for (int i = 0; i < 1000000; i++) {
        counter.update("foo");
      }
      assertEquals((long) expected.get(n), totalBytes.get());
    }
  }

  @Test
  public void testLazyByteCount() throws Exception {
    AtomicLong totalBytes = new AtomicLong();
    AtomicReference<ElementByteSizeObserver> capturedObserver = new AtomicReference<>();
    ElementByteSizeObservable<String> lazyObservable =
        new ElementByteSizeObservable<String>() {
          @Override
          public boolean isRegisterByteSizeObserverCheap(String value) {
            return true;
          }

          @Override
          public void registerByteSizeObserver(String value, ElementByteSizeObserver observer) {
            observer.setLazy();
            capturedObserver.set(observer);
          }
        };

    ElementByteSizeCounter counter =
        new ElementByteSizeCounter(lazyObservable, totalBytes::addAndGet, 1);
    counter.update("lazy-elem");
    assertEquals(0L, totalBytes.get());

    // Downstream consumes the lazy element before finishLazyUpdate is called.
    capturedObserver.get().update(27L);
    assertEquals(0L, totalBytes.get());

    counter.finishLazyUpdate("lazy-elem");
    assertEquals(27L, totalBytes.get());
  }

  @Test
  public void testExceptionDuringRegisterDiscardsPartialBytes() throws Exception {
    AtomicLong totalBytes = new AtomicLong();
    ElementByteSizeObservable<String> flakyObservable =
        new ElementByteSizeObservable<String>() {
          @Override
          public boolean isRegisterByteSizeObserverCheap(String value) {
            return true;
          }

          @Override
          public void registerByteSizeObserver(String value, ElementByteSizeObserver observer)
              throws Exception {
            if ("bad".equals(value)) {
              observer.setLazy();
              observer.update(50L);
              throw new CoderException("boom");
            }
            observer.update(12L);
          }
        };

    ElementByteSizeCounter counter =
        new ElementByteSizeCounter(flakyObservable, totalBytes::addAndGet, 1);
    assertThrows(CoderException.class, () -> counter.update("bad"));
    counter.finishLazyUpdate("bad");
    assertEquals(0L, totalBytes.get());

    counter.update("good");
    counter.finishLazyUpdate("good");
    assertEquals(12L, totalBytes.get());
  }
}
