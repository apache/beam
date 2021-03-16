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
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
abstract class BaseFirestoreFnTest<In, Out, Fn extends FirestoreDoFn<In, Out>> {

  protected final String projectId = "testing-project";

  @Rule
  public final Timeout timeout = new Timeout(10, TimeUnit.SECONDS);
  @Mock
  protected DoFn<In, Out>.StartBundleContext startBundleContext;
  @Mock
  protected DoFn<In, Out>.ProcessContext processContext;
  @Mock(lenient = true)
  protected DisplayData.Builder displayDataBuilder;
  @Mock(lenient = true)
  protected PipelineOptions pipelineOptions;
  @Mock(lenient = true)
  protected GcpOptions gcpOptions;
  @Mock(lenient = true)
  protected FirestoreIOOptions firestoreIOOptions;
  @Mock
  protected Credentials credentials;

  @Before
  public void stubDisplayDataBuilderChains() {
    when(displayDataBuilder.include(any(), any())).thenReturn(displayDataBuilder);
    when(displayDataBuilder.add(any())).thenReturn(displayDataBuilder);
    when(displayDataBuilder.addIfNotNull(any())).thenReturn(displayDataBuilder);
    when(displayDataBuilder.addIfNotDefault(any(), any())).thenReturn(displayDataBuilder);
  }

  @Before
  public void stubPipelineOptions() {
    when(startBundleContext.getPipelineOptions()).thenReturn(pipelineOptions);
    when(pipelineOptions.as(FirestoreIOOptions.class)).thenReturn(firestoreIOOptions);
    when(firestoreIOOptions.getEmulatorHostPort()).thenReturn(null);
    when(pipelineOptions.as(GcpOptions.class)).thenReturn(gcpOptions);
    when(gcpOptions.getProject()).thenReturn(projectId);
    when(gcpOptions.getGcpCredential()).thenReturn(credentials);
  }

  @Test
  public final void ensureSerializable() {
    SerializableUtils.ensureSerializable(getFn());
  }

  protected abstract Fn getFn();

  protected final void runFunction(Fn fn) throws Exception {
    runFunction(fn, 1);
  }

  protected final void runFunction(Fn fn, int processElementCount) throws Exception {
    fn.populateDisplayData(displayDataBuilder);
    fn.setup();
    fn.startBundle(startBundleContext);
    processElementsAndFinishBundle(fn, processElementCount);
  }

  protected abstract void processElementsAndFinishBundle(Fn fn, int processElementCount) throws Exception;

}
