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
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;

public class DataSamplingDescriptorModifier implements ProcessBundleDescriptorModifier {
  @Override
  public BeamFnApi.ProcessBundleDescriptor ModifyProcessBundleDescriptor(
      BeamFnApi.ProcessBundleDescriptor pbd) throws GraphModificationException {
    BeamFnApi.ProcessBundleDescriptor.Builder builder = pbd.toBuilder();
    for (String pcollectionId : pbd.getPcollectionsMap().keySet()) {
      RunnerApi.PCollection pcollection = pbd.getPcollectionsMap().get(pcollectionId);
      String coderId = pcollection.getCoderId();
      String transformId = "synthetic-data-sampling-transform-" + pcollectionId;
      try {
        builder.putTransforms(
            transformId,
            RunnerApi.PTransform.newBuilder()
                .setUniqueName(transformId)
                .setSpec(
                    RunnerApi.FunctionSpec.newBuilder()
                        .setUrn(DataSamplingFnRunner.URN)
                        .setPayload(
                            ByteString.copyFrom(
                                DataSamplingFnRunner.Payload.encode(pcollectionId, coderId))))
                .putInputs("main", pcollectionId)
                .build());
      } catch (Exception exception) {
        throw new GraphModificationException(
            "Failed to modify graph: could not encode payload for synthetic data "
                + "sampling operation.",
            exception);
      }
    }

    return builder.build();
  }
}
