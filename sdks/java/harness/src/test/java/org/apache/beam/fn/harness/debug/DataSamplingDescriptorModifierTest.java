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
package org.apache.beam.fn.harness.debug;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataSamplingDescriptorModifierTest {
  @Test
  public void testSimple() throws Exception {
    DataSamplingDescriptorModifier modifier = new DataSamplingDescriptorModifier();

    final String PCOLLECTION_ID = "pcollection-id";
    final String CODER_ID = "coder-id";
    BeamFnApi.ProcessBundleDescriptor descriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putPcollections(
                PCOLLECTION_ID,
                RunnerApi.PCollection.newBuilder()
                    .setUniqueName(PCOLLECTION_ID)
                    .setCoderId(CODER_ID)
                    .build())
            .putCoders(
                CODER_ID,
                RunnerApi.Coder.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DataSamplingFnRunner.URN))
                    .build())
            .build();

    BeamFnApi.ProcessBundleDescriptor modified = modifier.ModifyProcessBundleDescriptor(descriptor);
    assertThat(modified.getTransformsCount(), equalTo(1));

    RunnerApi.PTransform samplingTransform =
        Iterables.getOnlyElement(modified.getTransformsMap().values());
    assertThat(
        samplingTransform.getUniqueName(),
        equalTo("synthetic-data-sampling-transform-" + PCOLLECTION_ID));
    assertThat(samplingTransform.getSpec().getUrn(), equalTo(DataSamplingFnRunner.URN));
    assertThat(samplingTransform.getInputsMap(), hasEntry("main", PCOLLECTION_ID));
  }
}
