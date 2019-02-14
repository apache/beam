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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.worker.ByteStringCoder;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;

public class StateRequestHandlerImpl implements StateRequestHandler {

  private DataflowExecutionContext.DataflowStepContext ctxt;
  private ConcurrentHashMap<BeamFnApi.StateKey, BagState<ByteString>> userStateData;

  public StateRequestHandlerImpl(DataflowExecutionContext.DataflowStepContext ctxt) {
    this.ctxt = ctxt;
    this.userStateData = new ConcurrentHashMap<>();
  }

  @Override
  public CompletionStage<BeamFnApi.StateResponse.Builder> handle(BeamFnApi.StateRequest request)
      throws Exception {
    switch (request.getStateKey().getTypeCase()) {
      case BAG_USER_STATE:
        return handleBagUserState(request);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Dataflow does not handle StateRequests of type %s",
                request.getStateKey().getTypeCase()));
    }
  }

  public void finish() {
    userStateData.clear();
  }

  private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagUserState(
      BeamFnApi.StateRequest request) {
    BeamFnApi.StateKey.BagUserState bagUserStateKey = request.getStateKey().getBagUserState();
    // TODO: We should not be required to hold onto a pointer to the bag states for the
    // user. InMemoryStateInternals assumes that the Java garbage collector does the clean-up work
    // but instead StateInternals should hold its own references and write out any data and
    // clear references when the MapTask within Dataflow completes like how WindmillStateInternals
    // works.
    BagState<ByteString> state =
        userStateData.computeIfAbsent(
            request.getStateKey(),
            unusedKey ->
                ctxt.stateInternals()
                    .state(
                        // TODO: Once we have access to the ParDoPayload, use its windowing strategy
                        // to decode the window for the well known window types. Longer term we need
                        // to swap
                        // to use the encoded version and not rely on needing to decode the entire
                        // window.
                        StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE),
                        StateTags.bag(bagUserStateKey.getUserStateId(), ByteStringCoder.of())));
    switch (request.getRequestCase()) {
      case GET:
        return CompletableFuture.completedFuture(
            BeamFnApi.StateResponse.newBuilder()
                .setGet(BeamFnApi.StateGetResponse.newBuilder().setData(concat(state.read()))));
      case APPEND:
        state.add(request.getAppend().getData());
        return CompletableFuture.completedFuture(
            BeamFnApi.StateResponse.newBuilder()
                .setAppend(BeamFnApi.StateAppendResponse.getDefaultInstance()));
      case CLEAR:
        state.clear();
        return CompletableFuture.completedFuture(
            BeamFnApi.StateResponse.newBuilder()
                .setClear(BeamFnApi.StateClearResponse.getDefaultInstance()));
      default:
        throw new IllegalArgumentException(
            String.format("Unknown request type %s", request.getRequestCase()));
    }
  }

  private ByteString concat(Iterable<ByteString> values) {
    ByteString rval = ByteString.EMPTY;
    if (values != null) {
      for (ByteString value : values) {
        rval = rval.concat(value);
      }
    }
    return rval;
  }
}
