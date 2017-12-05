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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link SdkHarnessDoFnRunner}. */
@RunWith(JUnit4.class)
public class SdkHarnessDoFnRunnerTest {
  @Mock private SdkHarnessClient mockClient;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testStartAndFinishBundleDoesNotCrash() {
    String processBundleDescriptorId = "testDescriptor";
    String bundleId = "testBundle";
    SdkHarnessDoFnRunner<Void, Void> underTest =
        SdkHarnessDoFnRunner.<Void, Void>create(mockClient, processBundleDescriptorId);

    SettableFuture<BeamFnApi.ProcessBundleResponse> processBundleResponseFuture =
        SettableFuture.create();
    FnDataReceiver dummyInputReceiver = new FnDataReceiver() {
      @Override
      public void accept(Object input) throws Exception {
        fail("Dummy input receiver should not have received data");
      }

      @Override
      public void close() throws IOException {
        // noop
      }
    };
    SdkHarnessClient.ActiveBundle activeBundle =
        SdkHarnessClient.ActiveBundle.create(
            bundleId, processBundleResponseFuture, dummyInputReceiver);

    when(mockClient.newBundle(anyString())).thenReturn(activeBundle);
    underTest.startBundle();
    processBundleResponseFuture.set(BeamFnApi.ProcessBundleResponse.getDefaultInstance());
    underTest.finishBundle();
  }
}
