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
package org.apache.beam.runners.fnexecution.control;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
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

    ProcessBundleDescriptor descriptor1 =
        ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build();
    ProcessBundleDescriptor descriptor2 =
        ProcessBundleDescriptor.newBuilder().setId(descriptorId2).build();
    Map<String, BundleProcessor> responseFuture =
        sdkHarnessClient.register(ImmutableList.of(descriptor1, descriptor2));

    // Correlating the RegisterRequest and RegisterResponse is owned by the underlying
    // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
    // the response.
    //
    // Currently there are no fields so there's nothing to check. This test is formulated
    // to match the pattern it should have if/when the response is meaningful.
    assertThat(
        responseFuture.keySet(), containsInAnyOrder(descriptor1.getId(), descriptor2.getId()));
  }

  @Test
  public void testNewBundleNoDataDoesNotCrash() throws Exception {
    String descriptorId1 = "descriptor1";

    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build();
    SettableFuture<BeamFnApi.InstructionResponse> processBundleResponseFuture =
        SettableFuture.create();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(SettableFuture.<InstructionResponse>create())
        .thenReturn(processBundleResponseFuture);

    BundleProcessor processor =
        sdkHarnessClient.getProcessor(descriptor);
    SdkHarnessClient.ActiveBundle activeBundle = processor.newBundle();

    // Correlating the ProcessBundleRequest and ProcessBundleResponse is owned by the underlying
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
