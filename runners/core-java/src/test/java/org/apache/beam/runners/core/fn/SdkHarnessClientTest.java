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
package org.apache.beam.runners.core.fn;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.Future;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link SdkHarnessClient}. */
@RunWith(JUnit4.class)
public class SdkHarnessClientTest {

  @Mock public FnApiControlClient fnApiControlClient;

  private SdkHarnessClient sdkHarnessClient;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    sdkHarnessClient = SdkHarnessClient.usingFnApiClient(fnApiControlClient);
  }

  @Test
  public void testRegisterDoesNotCrash() throws Exception {
    String descriptorId1 = "descriptor1";
    String descriptorId2 = "descriptor2";

    SettableFuture<BeamFnApi.InstructionResponse> registerResponseFuture = SettableFuture.create();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(registerResponseFuture);

    Future<BeamFnApi.RegisterResponse> responseFuture = sdkHarnessClient.register(
        ImmutableList.of(
            BeamFnApi.ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build(),
            BeamFnApi.ProcessBundleDescriptor.newBuilder().setId(descriptorId2).build()));

    // Correlating the RegisterRequest and RegisterResponse is owned by the underlying
    // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
    // the response.
    //
    // Currently there are no fields so there's nothing to check. This test is formulated
    // to match the pattern it should have if/when the response is meaningful.
    BeamFnApi.RegisterResponse response = BeamFnApi.RegisterResponse.getDefaultInstance();
    registerResponseFuture.set(
        BeamFnApi.InstructionResponse.newBuilder().setRegister(response).build());
    responseFuture.get();
  }

  @Test
  public void testNewBundleNoDataDoesNotCrash() throws Exception {
    String descriptorId1 = "descriptor1";

    SettableFuture<BeamFnApi.InstructionResponse> processBundleResponseFuture =
        SettableFuture.create();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(processBundleResponseFuture);

    SdkHarnessClient.ActiveBundle activeBundle = sdkHarnessClient.newBundle(descriptorId1);

    // Correlating the ProcessBundleRequest and ProcessBundleReponse is owned by the underlying
    // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
    // the response.
    //
    // Currently there are no fields so there's nothing to check. This test is formulated
    // to match the pattern it should have if/when the response is meaningful.
    BeamFnApi.ProcessBundleResponse response = BeamFnApi.ProcessBundleResponse.getDefaultInstance();
    processBundleResponseFuture.set(
        BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build());
    activeBundle.getBundleResponse().get();
  }
}
