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

import org.apache.beam.fn.harness.ProcessBundleDescriptorModifier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;

/**
 * Modifies the given ProcessBundleDescriptor by adding a DataSampling operation as a consumer to
 * every PCollection.
 */
public class DataSamplingDescriptorModifier implements ProcessBundleDescriptorModifier {
  @Override
  public BeamFnApi.ProcessBundleDescriptor modifyProcessBundleDescriptor(
      BeamFnApi.ProcessBundleDescriptor pbd) {
    BeamFnApi.ProcessBundleDescriptor.Builder builder = pbd.toBuilder();

    // Get all PCollections to modify.
    for (String pcollectionId : pbd.getPcollectionsMap().keySet()) {
      // Create a new DataSampling PTransform that consumes that given PCollection.
      String transformId = "synthetic-data-sampling-transform-" + pcollectionId;
      builder.putTransforms(
          transformId,
          RunnerApi.PTransform.newBuilder()
              .setUniqueName(transformId)
              .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DataSamplingFnRunner.URN))
              .putInputs("main", pcollectionId)
              .build());
    }

    return builder.build();
  }
}
