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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import static org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.*;

import java.io.InputStream;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequestOverlay;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.MethodDescriptor.Marshaller;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.protobuf.ProtoUtils;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.AbstractStub;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.ClientCalls;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;

public class CloudWindmillServiceV1Alpha1CustomStub
    extends AbstractStub<CloudWindmillServiceV1Alpha1CustomStub> {

  private static final MethodDescriptor<CommitRequest, StreamingCommitResponse>
      COMMIT_METHOD_DESCRIPTOR =
          MethodDescriptor.<CommitRequest, StreamingCommitResponse>newBuilder()
              .setFullMethodName(getCommitWorkStreamMethod().getFullMethodName())
              .setIdempotent(getCommitWorkStreamMethod().isIdempotent())
              .setRequestMarshaller(new CommitRequestMarshaller())
              .setSafe(getCommitWorkStreamMethod().isSafe())
              .setType(getCommitWorkStreamMethod().getType())
              .setResponseMarshaller(getCommitWorkStreamMethod().getResponseMarshaller())
              .setSchemaDescriptor(getCommitWorkStreamMethod().getSchemaDescriptor())
              .setSampledToLocalTracing(getCommitWorkStreamMethod().isSampledToLocalTracing())
              .build();

  private CloudWindmillServiceV1Alpha1CustomStub(Channel channel, CallOptions callOptions) {
    super(channel, callOptions);
  }

  @Override
  protected CloudWindmillServiceV1Alpha1CustomStub build(Channel channel, CallOptions callOptions) {
    return new CloudWindmillServiceV1Alpha1CustomStub(channel, callOptions);
  }

  /** Creates a new CloudWindmillServiceV1Alpha1CustomStub */
  public static CloudWindmillServiceV1Alpha1CustomStub newStub(Channel channel) {
    return CloudWindmillServiceV1Alpha1CustomStub.newStub(
        CloudWindmillServiceV1Alpha1CustomStub::new, channel);
  }

  /** Streams commit of previously acquired work. Response is a stream. */
  public StreamObserver<CommitRequest> commitWorkStream(
      StreamObserver<StreamingCommitResponse> responseObserver) {
    return ClientCalls.asyncBidiStreamingCall(
        getChannel().newCall(COMMIT_METHOD_DESCRIPTOR, getCallOptions()), responseObserver);
  }

  /** Gets streaming dataflow work. Response is a stream. */
  public StreamObserver<StreamingGetWorkRequest> getWorkStream(
      StreamObserver<StreamingGetWorkResponseChunk> responseObserver) {
    return ClientCalls.asyncBidiStreamingCall(
        getChannel().newCall(getGetWorkStreamMethod(), getCallOptions()), responseObserver);
  }

  /** Gets data from windmill. Response is a stream. */
  public StreamObserver<
          org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetDataRequest>
      getDataStream(
          StreamObserver<
                  org.apache.beam.runners.dataflow.worker.windmill.Windmill
                      .StreamingGetDataResponse>
              responseObserver) {
    return ClientCalls.asyncBidiStreamingCall(
        getChannel().newCall(getGetDataStreamMethod(), getCallOptions()), responseObserver);
  }

  // Holder class containing either StreamingCommitWorkRequest or
  // StreamingCommitWorkRequestOverlay.
  public static class CommitRequest {
    @Nullable final StreamingCommitWorkRequest request;
    @Nullable final StreamingCommitWorkRequestOverlay overlay;

    public CommitRequest(StreamingCommitWorkRequest request) {
      this.request = request;
      this.overlay = null;
    }

    public CommitRequest(StreamingCommitWorkRequestOverlay overlay) {
      this.request = null;
      this.overlay = overlay;
    }

    @Nullable
    public StreamingCommitWorkRequest getRequest() {
      return request;
    }

    @Nullable
    public StreamingCommitWorkRequestOverlay getOverlay() {
      return overlay;
    }
  }

  // Both protos have identical wire format and serialized StreamingCommitWorkRequestOverlay
  // can be deserialized into StreamingCommitWorkRequest.
  private static class CommitRequestMarshaller implements Marshaller<CommitRequest> {
    private static final Marshaller<StreamingCommitWorkRequestOverlay> OVERLAY_MARSHELLER =
        ProtoUtils.marshaller(StreamingCommitWorkRequestOverlay.getDefaultInstance());
    private static final Marshaller<StreamingCommitWorkRequest> REQUEST_MARSHALLER =
        ProtoUtils.marshaller(StreamingCommitWorkRequest.getDefaultInstance());

    @Override
    public InputStream stream(CommitRequest commitRequest) {
      if (commitRequest.request != null) {
        return REQUEST_MARSHALLER.stream(commitRequest.request);
      }
      return OVERLAY_MARSHELLER.stream(commitRequest.overlay);
    }

    @Override
    public CommitRequest parse(InputStream inputStream) {
      return new CommitRequest(REQUEST_MARSHALLER.parse(inputStream));
    }
  }
}
