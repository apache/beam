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
package org.apache.beam.runners.samza.runtime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@linkplain FutureCollectorImpl}. */
public final class FutureCollectorImplTest {
  private static final List<String> RESULTS = ImmutableList.of("hello", "world");
  private FutureCollector<String> futureCollector = new FutureCollectorImpl<>();

  @Before
  public void setup() {
    futureCollector = new FutureCollectorImpl<>();
  }

  @Test(expected = IllegalStateException.class)
  public void testAddWithoutPrepareCallThrowsException() {
    futureCollector.add(mock(CompletionStage.class));
  }

  @Test
  public void testFinishWithoutPrepareReturnsEmptyCollection() {
    CompletionStage<Collection<WindowedValue<String>>> resultFuture = futureCollector.finish();
    CompletionStage<Void> validationFuture =
        resultFuture.thenAccept(
            result -> {
              Assert.assertTrue("Expected the result to be empty", result.isEmpty());
            });
    validationFuture.toCompletableFuture().join();
  }

  @Test
  public void testFinishReturnsExpectedResults() {
    WindowedValue<String> mockWindowedValue = mock(WindowedValue.class);

    when(mockWindowedValue.getValue()).thenReturn("hello").thenReturn("world");

    futureCollector.prepare();
    futureCollector.add(CompletableFuture.completedFuture(mockWindowedValue));
    futureCollector.add(CompletableFuture.completedFuture(mockWindowedValue));

    CompletionStage<Collection<WindowedValue<String>>> resultFuture = futureCollector.finish();
    CompletionStage<Void> validationFuture =
        resultFuture.thenAccept(
            results -> {
              List<String> actualResults =
                  results.stream().map(WindowedValue::getValue).collect(Collectors.toList());
              Assert.assertEquals(
                  "Expected the result to be {hello, world}", RESULTS, actualResults);
            });
    validationFuture.toCompletableFuture().join();
  }

  @Test
  public void testMultiplePrepareCallsWithoutFinishThrowsException() {
    futureCollector.prepare();

    try {
      futureCollector.prepare();
      Assert.fail("Second invocation of prepare should throw IllegalStateException");
    } catch (IllegalStateException ex) {
    }
  }
}
