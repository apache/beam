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

  /**
   * Tests that given a ProcessBundleDescriptor, the correct graph modification is done to create a DataSampling PTransform.
   */
  @Test
  public void testThatDataSamplingTransformIsMade() {
    DataSamplingDescriptorModifier modifier = new DataSamplingDescriptorModifier();

    final String PCOLLECTION_ID_A = "pcollection-id-a";
    final String PCOLLECTION_ID_B = "pcollection-id-b";
    final String CODER_ID_A = "coder-id-a";
    final String CODER_ID_B = "coder-id-b";
    BeamFnApi.ProcessBundleDescriptor descriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putPcollections(
                PCOLLECTION_ID_A,
                RunnerApi.PCollection.newBuilder()
                    .setUniqueName(PCOLLECTION_ID_A)
                    .setCoderId(CODER_ID_A)
                    .build())
            .putPcollections(
                PCOLLECTION_ID_B,
                RunnerApi.PCollection.newBuilder()
                    .setUniqueName(PCOLLECTION_ID_B)
                    .setCoderId(CODER_ID_B)
                    .build())
            .putCoders(
                CODER_ID_A,
                RunnerApi.Coder.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DataSamplingFnRunner.URN))
                    .build())
            .putCoders(
                CODER_ID_B,
                RunnerApi.Coder.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DataSamplingFnRunner.URN))
                    .build())
            .build();

    final String PTRANSFORM_ID_A = "synthetic-data-sampling-transform-" + PCOLLECTION_ID_A;
    final String PTRANSFORM_ID_B = "synthetic-data-sampling-transform-" + PCOLLECTION_ID_B;

    BeamFnApi.ProcessBundleDescriptor modified = modifier.ModifyProcessBundleDescriptor(descriptor);
    assertThat(modified.getTransformsCount(), equalTo(2));

    RunnerApi.PTransform samplingTransformA = modified.getTransformsMap().get(PTRANSFORM_ID_A);
    assertThat(samplingTransformA.getUniqueName(), equalTo(PTRANSFORM_ID_A));
    assertThat(samplingTransformA.getSpec().getUrn(), equalTo(DataSamplingFnRunner.URN));
    assertThat(samplingTransformA.getInputsMap(), hasEntry("main", PCOLLECTION_ID_A));

    RunnerApi.PTransform samplingTransformB = modified.getTransformsMap().get(PTRANSFORM_ID_B);
    assertThat(samplingTransformB.getUniqueName(), equalTo(PTRANSFORM_ID_B));
    assertThat(samplingTransformB.getSpec().getUrn(), equalTo(DataSamplingFnRunner.URN));
    assertThat(samplingTransformB.getInputsMap(), hasEntry("main", PCOLLECTION_ID_B));
  }
}
