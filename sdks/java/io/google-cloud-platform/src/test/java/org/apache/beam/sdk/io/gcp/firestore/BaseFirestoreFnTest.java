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
package org.apache.beam.sdk.io.gcp.firestore;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.auth.Credentials;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings(
    "initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
abstract class BaseFirestoreFnTest<InT, OutT, FnT extends FirestoreDoFn<InT, OutT>> {

  protected final String projectId = "testing-project";

  // @Rule public final Timeout timeout = new Timeout(10, TimeUnit.SECONDS);
  @Mock protected DoFn<InT, OutT>.StartBundleContext startBundleContext;
  @Mock protected DoFn<InT, OutT>.ProcessContext processContext;

  @Mock(lenient = true)
  protected DisplayData.Builder displayDataBuilder;

  @Mock protected Credentials credentials;

  protected PipelineOptions pipelineOptions;

  @Before
  public void stubDisplayDataBuilderChains() {
    when(displayDataBuilder.include(any(), any())).thenReturn(displayDataBuilder);
    when(displayDataBuilder.add(any())).thenReturn(displayDataBuilder);
    when(displayDataBuilder.addIfNotNull(any())).thenReturn(displayDataBuilder);
    when(displayDataBuilder.addIfNotDefault(any(), any())).thenReturn(displayDataBuilder);
  }

  @Before
  public void stubPipelineOptions() {
    pipelineOptions = TestPipeline.testingPipelineOptions();
    pipelineOptions.as(GcpOptions.class).setProject(projectId);
    pipelineOptions.as(GcpOptions.class).setGcpCredential(credentials);

    when(startBundleContext.getPipelineOptions()).thenReturn(pipelineOptions);
  }

  @Test
  public final void ensureSerializable() {
    SerializableUtils.ensureSerializable(getFn());
  }

  protected abstract FnT getFn();

  protected final void runFunction(FnT fn) throws Exception {
    runFunction(fn, 1);
  }

  protected final void runFunction(FnT fn, int processElementCount) throws Exception {
    fn.populateDisplayData(displayDataBuilder);
    fn.setup();
    fn.startBundle(startBundleContext);
    processElementsAndFinishBundle(fn, processElementCount);
  }

  protected abstract void processElementsAndFinishBundle(FnT fn, int processElementCount)
      throws Exception;
}
