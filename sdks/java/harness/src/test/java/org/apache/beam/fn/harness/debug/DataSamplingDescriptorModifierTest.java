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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataSamplingDescriptorModifierTest {

  /**
   * Tests that given a ProcessBundleDescriptor, the correct graph modification is done to create a
   * DataSampling PTransform.
   */
  @Test
  public void testThatDataSamplingTransformIsMade() {
    DataSamplingDescriptorModifier modifier = new DataSamplingDescriptorModifier();

    final String pcollectionIdA = "pcollection-id-a";
    final String pcollectionIdB = "pcollection-id-b";
    final String coderIdA = "coder-id-a";
    final String coderIdB = "coder-id-b";
    BeamFnApi.ProcessBundleDescriptor descriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putPcollections(
                pcollectionIdA,
                RunnerApi.PCollection.newBuilder()
                    .setUniqueName(pcollectionIdA)
                    .setCoderId(coderIdA)
                    .build())
            .putPcollections(
                pcollectionIdB,
                RunnerApi.PCollection.newBuilder()
                    .setUniqueName(pcollectionIdB)
                    .setCoderId(coderIdB)
                    .build())
            .putCoders(
                coderIdA,
                RunnerApi.Coder.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DataSamplingFnRunner.URN))
                    .build())
            .putCoders(
                coderIdB,
                RunnerApi.Coder.newBuilder()
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DataSamplingFnRunner.URN))
                    .build())
            .build();

    final String ptransformIdA = "synthetic-data-sampling-transform-" + pcollectionIdA;
    final String ptransformIdB = "synthetic-data-sampling-transform-" + pcollectionIdB;

    BeamFnApi.ProcessBundleDescriptor modified = modifier.modifyProcessBundleDescriptor(descriptor);
    assertThat(modified.getTransformsCount(), equalTo(2));

    RunnerApi.PTransform samplingTransformA = modified.getTransformsMap().get(ptransformIdA);
    assertThat(samplingTransformA.getUniqueName(), equalTo(ptransformIdA));
    assertThat(samplingTransformA.getSpec().getUrn(), equalTo(DataSamplingFnRunner.URN));
    assertThat(samplingTransformA.getInputsMap(), hasEntry("main", pcollectionIdA));

    RunnerApi.PTransform samplingTransformB = modified.getTransformsMap().get(ptransformIdB);
    assertThat(samplingTransformB.getUniqueName(), equalTo(ptransformIdB));
    assertThat(samplingTransformB.getSpec().getUrn(), equalTo(DataSamplingFnRunner.URN));
    assertThat(samplingTransformB.getInputsMap(), hasEntry("main", pcollectionIdB));
  }
}
