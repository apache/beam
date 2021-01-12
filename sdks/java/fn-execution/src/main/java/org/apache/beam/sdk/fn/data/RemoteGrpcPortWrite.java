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
package org.apache.beam.sdk.fn.data;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/**
 * An execution-time only {@link PTransform} which represents a write from within an SDK harness to
 * a {@link RemoteGrpcPort}.
 */
@AutoValue
public abstract class RemoteGrpcPortWrite {
  public static final String URN = "beam:runner:sink:v1";
  private static final String LOCAL_INPUT_ID = "local_input";

  /**
   * Create a {@link RemoteGrpcPortWrite} which writes the {@link PCollection} with the provided
   * Pipeline id to the provided {@link RemoteGrpcPort}.
   */
  public static RemoteGrpcPortWrite writeToPort(String inputPCollectionId, RemoteGrpcPort port) {
    return new AutoValue_RemoteGrpcPortWrite(inputPCollectionId, port);
  }

  public static RemoteGrpcPortWrite fromPTransform(PTransform pTransform)
      throws InvalidProtocolBufferException {
    checkArgument(
        URN.equals(pTransform.getSpec().getUrn()),
        "Expected URN for %s, got %s",
        RemoteGrpcPortWrite.class.getSimpleName(),
        pTransform.getSpec().getUrn());
    checkArgument(
        pTransform.getInputsCount() == 1,
        "Expected exactly one output, got %s",
        pTransform.getOutputsCount());
    RemoteGrpcPort port = RemoteGrpcPort.parseFrom(pTransform.getSpec().getPayload());
    String inputPCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());
    return writeToPort(inputPCollectionId, port);
  }

  abstract String getInputPCollectionId();

  public abstract RemoteGrpcPort getPort();

  public PTransform toPTransform() {
    return PTransform.newBuilder()
        .setSpec(FunctionSpec.newBuilder().setUrn(URN).setPayload(getPort().toByteString()).build())
        .putInputs(LOCAL_INPUT_ID, getInputPCollectionId())
        .build();
  }
}
