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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver;
import org.apache.beam.sdk.fn.data.CompletableFutureInboundDataClient;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnDataInboundObserver}. */
@RunWith(JUnit4.class)
public class BeamFnDataInboundObserverTest {
  private static final Coder<WindowedValue<String>> CODER =
      WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDecodingElements() throws Exception {
    Collection<WindowedValue<String>> values = new ArrayList<>();
    InboundDataClient readFuture = CompletableFutureInboundDataClient.create();
    BeamFnDataInboundObserver<String> observer =
        new BeamFnDataInboundObserver<>(CODER, values::add, readFuture);

    // Test decoding multiple messages
    observer.accept(dataWith("ABC", "DEF", "GHI"));
    assertThat(values, contains(
        valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF"), valueInGlobalWindow("GHI")));
    values.clear();

    // Test empty message signaling end of stream
    assertFalse(readFuture.isDone());
    observer.accept(dataWith());
    assertTrue(readFuture.isDone());

    // Test messages after stream is finished are discarded
    observer.accept(dataWith("ABC", "DEF", "GHI"));
    assertThat(values, empty());
  }

  @Test
  public void testConsumptionFailureCompletesReadFutureAndDiscardsMessages() throws Exception {
    InboundDataClient readClient = CompletableFutureInboundDataClient.create();
    BeamFnDataInboundObserver<String> observer =
        new BeamFnDataInboundObserver<>(CODER, this::throwOnDefValue, readClient);

    assertFalse(readClient.isDone());
    observer.accept(dataWith("ABC", "DEF", "GHI"));
    assertTrue(readClient.isDone());

    thrown.expect(ExecutionException.class);
    thrown.expectCause(instanceOf(RuntimeException.class));
    thrown.expectMessage("Failure");
    readClient.awaitCompletion();
  }

  private void throwOnDefValue(WindowedValue<String> value) {
    if ("DEF".equals(value.getValue())) {
      throw new RuntimeException("Failure");
    }
  }

  private BeamFnApi.Elements.Data dataWith(String ... values) throws Exception {
    BeamFnApi.Elements.Data.Builder builder = BeamFnApi.Elements.Data.newBuilder()
        .setInstructionReference("777L")
        .setTarget(BeamFnApi.Target.newBuilder()
            .setPrimitiveTransformReference("999L")
            .setName("Test"));
    ByteString.Output output = ByteString.newOutput();
    for (String value : values) {
      CODER.encode(valueInGlobalWindow(value), output);
    }
    builder.setData(output.toByteString());
    return builder.build();
  }
}
