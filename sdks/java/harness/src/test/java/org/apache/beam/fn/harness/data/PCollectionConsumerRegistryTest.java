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
package org.apache.beam.fn.harness.data;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PCollectionConsumerRegistryTest}. */
@RunWith(JUnit4.class)
public class PCollectionConsumerRegistryTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  /**
   * Test that the counter increments only once when multiple consumers of same pCollection read the
   * same element.
   */
  @Test
  public void multipleConsumersSamePCollection() throws Exception {
    final String pCollectionA = "pCollectionA";

    PCollectionConsumerRegistry consumers = new PCollectionConsumerRegistry();
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(pCollectionA, consumerA1);
    consumers.register(pCollectionA, consumerA2);

    FnDataReceiver<WindowedValue<String>> wrapperConsumer =
        (FnDataReceiver<WindowedValue<String>>)
            (FnDataReceiver) consumers.getMultiplexingConsumer(pCollectionA);

    WindowedValue<String> element = WindowedValue.valueInGlobalWindow("elem");
    int numElements = 20;
    for (int i = 0; i < numElements; i++) {
      wrapperConsumer.accept(element);
    }

    // Check that the underlying consumers are each invoked per element.
    verify(consumerA1, times(numElements)).accept(element);
    verify(consumerA2, times(numElements)).accept(element);

    assertThat(consumers.keySet(), contains(pCollectionA));
    assertThat(consumers.getUnderlyingConsumers(pCollectionA), contains(consumerA1, consumerA2));
  }

  @Test
  public void throwsOnRegisteringAfterMultiplexingConsumerWasInitialized() throws Exception {
    final String pCollectionA = "pCollectionA";

    PCollectionConsumerRegistry consumers = new PCollectionConsumerRegistry();
    FnDataReceiver<WindowedValue<String>> consumerA1 = mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<String>> consumerA2 = mock(FnDataReceiver.class);

    consumers.register(pCollectionA, consumerA1);
    consumers.getMultiplexingConsumer(pCollectionA);

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("cannot be register()-d after");
    consumers.register(pCollectionA, consumerA2);
  }
}
