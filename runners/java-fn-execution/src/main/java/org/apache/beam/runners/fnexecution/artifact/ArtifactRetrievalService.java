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
package org.apache.beam.runners.fnexecution.artifact;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;

/** An {@link ArtifactRetrievalService} that uses {@link FileSystems} as its backing storage. */
public class ArtifactRetrievalService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase implements FnService {

  public static final int DEFAULT_BUFFER_SIZE = 4 << 20; // 4 MB

  public static final String FILE_ARTIFACT_URN = "beam:artifact:type:file:v1";
  public static final String URL_ARTIFACT_URN = "beam:artifact:type:url:v1";
  public static final String STAGING_TO_ARTIFACT_URN = "beam:artifact:role:staging_to:v1";

  static {
    checkState(FILE_ARTIFACT_URN.equals(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE)));
    checkState(URL_ARTIFACT_URN.equals(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.URL)));
    checkState(
        STAGING_TO_ARTIFACT_URN.equals(
            BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO)));
  }

  private final int bufferSize;

  public ArtifactRetrievalService() {
    this(DEFAULT_BUFFER_SIZE);
  }

  public ArtifactRetrievalService(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  @Override
  public void resolveArtifacts(
      ArtifactApi.ResolveArtifactsRequest request,
      StreamObserver<ArtifactApi.ResolveArtifactsResponse> responseObserver) {
    responseObserver.onNext(
        ArtifactApi.ResolveArtifactsResponse.newBuilder()
            .addAllReplacements(request.getArtifactsList())
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getArtifact(
      ArtifactApi.GetArtifactRequest request,
      StreamObserver<ArtifactApi.GetArtifactResponse> responseObserver) {
    switch (request.getArtifact().getTypeUrn()) {
      case FILE_ARTIFACT_URN:
        try {
          RunnerApi.ArtifactFilePayload payload =
              RunnerApi.ArtifactFilePayload.parseFrom(request.getArtifact().getTypePayload());
          InputStream inputStream =
              Channels.newInputStream(
                  FileSystems.open(
                      FileSystems.matchNewResource(payload.getPath(), false /* is directory */)));
          byte[] buffer = new byte[bufferSize];
          int bytesRead;
          while ((bytesRead = inputStream.read(buffer)) > 0) {
            responseObserver.onNext(
                ArtifactApi.GetArtifactResponse.newBuilder()
                    .setData(ByteString.copyFrom(buffer, 0, bytesRead))
                    .build());
          }
          responseObserver.onCompleted();
        } catch (IOException exn) {
          exn.printStackTrace();
          responseObserver.onError(exn);
        }
        break;
      default:
        responseObserver.onError(
            new StatusException(
                Status.INVALID_ARGUMENT.withDescription(
                    "Unexpected artifact type: " + request.getArtifact().getTypeUrn())));
    }
  }

  @Override
  public void close() {
    // Nothing to close.
  }
}
