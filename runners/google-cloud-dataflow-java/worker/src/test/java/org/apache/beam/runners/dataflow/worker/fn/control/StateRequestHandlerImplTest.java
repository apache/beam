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
package org.apache.beam.runners.dataflow.worker.fn.control;

import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.joda.time.Instant;
import org.junit.Test;

public class StateRequestHandlerImplTest {

  @Test
  public void testBagUserStateAppend() throws Exception {
    StateRequestHandlerImpl handler = new StateRequestHandlerImpl(buildDataflowStepContext());

    String expectedData = "expectedData";
    String actualData = "bad";
    String key = "key";

    ByteString.Output windowOutput = ByteString.newOutput();
    GlobalWindow.Coder.INSTANCE.encode(GlobalWindow.INSTANCE, windowOutput);
    ByteString window = windowOutput.toByteString();

    {
      BeamFnApi.StateRequest appendRequest =
          BeamFnApi.StateRequest.newBuilder()
              .setId("requestId")
              .setInstructionReference("instructionReference")
              .setStateKey(
                  BeamFnApi.StateKey.newBuilder()
                      .setBagUserState(
                          BeamFnApi.StateKey.BagUserState.newBuilder()
                              .setPtransformId("ptransformId")
                              .setUserStateId("userStateId")
                              .setWindow(window)
                              .setKey(encodeToByteString(key))
                              .build()))
              .setAppend(
                  BeamFnApi.StateAppendRequest.newBuilder()
                      .setData(encodeToByteString(expectedData))
                      .build())
              .build();

      handler.handle(appendRequest).toCompletableFuture().join();
    }

    {
      BeamFnApi.StateRequest getRequest =
          BeamFnApi.StateRequest.newBuilder()
              .setId("requestId")
              .setInstructionReference("instructionReference")
              .setStateKey(
                  BeamFnApi.StateKey.newBuilder()
                      .setBagUserState(
                          BeamFnApi.StateKey.BagUserState.newBuilder()
                              .setPtransformId("ptransformId")
                              .setUserStateId("userStateId")
                              .setWindow(window)
                              .setKey(encodeToByteString(key))
                              .build()))
              .setGet(BeamFnApi.StateGetRequest.getDefaultInstance())
              .build();

      actualData =
          decodeFromByteString(
              handler.handle(getRequest).toCompletableFuture().get().build().getGet().getData());
    }
    org.junit.Assert.assertEquals(expectedData, actualData);
  }

  private ByteString encodeToByteString(String data) throws Exception {
    StringUtf8Coder coder = StringUtf8Coder.of();
    ByteString.Output output = ByteString.newOutput();
    coder.encode("expectedData", output);
    return output.toByteString();
  }

  private String decodeFromByteString(ByteString data) throws Exception {
    StringUtf8Coder coder = StringUtf8Coder.of();
    return coder.decode(data.newInput());
  }

  private static class TestStepContext extends DataflowExecutionContext.DataflowStepContext {
    private InMemoryStateInternals stateInternals;

    public TestStepContext(NameContext nameContext) {
      super(nameContext);

      stateInternals = InMemoryStateInternals.forKey("unused");
    }

    @Nullable
    @Override
    public <W extends BoundedWindow> TimerInternals.TimerData getNextFiredTimer(
        Coder<W> windowCoder) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DataflowExecutionContext.DataflowStepContext namespacedToUser() {
      return this;
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <W extends BoundedWindow> void setStateCleanupTimer(
        String timerId, W window, Coder<W> windowCoder, Instant cleanupTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public StateInternals stateInternals() {
      return stateInternals;
    }
  }

  private static DataflowExecutionContext.DataflowStepContext buildDataflowStepContext() {
    return new TestStepContext(NameContext.create("", "", "", ""));
  }
}
