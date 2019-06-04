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
package org.apache.beam.runners.fnexecution.state;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.EnumMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.MultimapSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for {@link StateRequestHandlers}. */
@RunWith(JUnit4.class)
public class StateRequestHandlersTest {
  @Test
  public void testDelegatingStateHandlerDelegates() throws Exception {
    StateRequestHandler mockHandler = Mockito.mock(StateRequestHandler.class);
    StateRequestHandler mockHandler2 = Mockito.mock(StateRequestHandler.class);
    EnumMap<StateKey.TypeCase, StateRequestHandler> handlers =
        new EnumMap<>(StateKey.TypeCase.class);
    handlers.put(StateKey.TypeCase.TYPE_NOT_SET, mockHandler);
    handlers.put(TypeCase.MULTIMAP_SIDE_INPUT, mockHandler2);
    StateRequest request = StateRequest.getDefaultInstance();
    StateRequest request2 =
        StateRequest.newBuilder()
            .setStateKey(
                StateKey.newBuilder().setMultimapSideInput(MultimapSideInput.getDefaultInstance()))
            .build();
    StateRequestHandlers.delegateBasedUponType(handlers).handle(request);
    StateRequestHandlers.delegateBasedUponType(handlers).handle(request2);
    verify(mockHandler).handle(request);
    verify(mockHandler2).handle(request2);
    verifyNoMoreInteractions(mockHandler, mockHandler2);
  }

  @Test
  public void testDelegatingStateHandlerThrowsWhenNotFound() throws Exception {
    StateRequestHandlers.delegateBasedUponType(new EnumMap<>(StateKey.TypeCase.class))
        .handle(StateRequest.getDefaultInstance());
  }
}
