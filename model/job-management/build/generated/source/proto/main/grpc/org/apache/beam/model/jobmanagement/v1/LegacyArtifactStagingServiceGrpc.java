package org.apache.beam.model.jobmanagement.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * A service to stage artifacts for use in a Job.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/job_management/v1/beam_artifact_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class LegacyArtifactStagingServiceGrpc {

  private LegacyArtifactStagingServiceGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.job_management.v1.LegacyArtifactStagingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse> getPutArtifactMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PutArtifact",
      requestType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse> getPutArtifactMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse> getPutArtifactMethod;
    if ((getPutArtifactMethod = LegacyArtifactStagingServiceGrpc.getPutArtifactMethod) == null) {
      synchronized (LegacyArtifactStagingServiceGrpc.class) {
        if ((getPutArtifactMethod = LegacyArtifactStagingServiceGrpc.getPutArtifactMethod) == null) {
          LegacyArtifactStagingServiceGrpc.getPutArtifactMethod = getPutArtifactMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PutArtifact"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LegacyArtifactStagingServiceMethodDescriptorSupplier("PutArtifact"))
              .build();
        }
      }
    }
    return getPutArtifactMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse> getCommitManifestMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CommitManifest",
      requestType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse> getCommitManifestMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse> getCommitManifestMethod;
    if ((getCommitManifestMethod = LegacyArtifactStagingServiceGrpc.getCommitManifestMethod) == null) {
      synchronized (LegacyArtifactStagingServiceGrpc.class) {
        if ((getCommitManifestMethod = LegacyArtifactStagingServiceGrpc.getCommitManifestMethod) == null) {
          LegacyArtifactStagingServiceGrpc.getCommitManifestMethod = getCommitManifestMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CommitManifest"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LegacyArtifactStagingServiceMethodDescriptorSupplier("CommitManifest"))
              .build();
        }
      }
    }
    return getCommitManifestMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static LegacyArtifactStagingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactStagingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactStagingServiceStub>() {
        @java.lang.Override
        public LegacyArtifactStagingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LegacyArtifactStagingServiceStub(channel, callOptions);
        }
      };
    return LegacyArtifactStagingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static LegacyArtifactStagingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactStagingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactStagingServiceBlockingStub>() {
        @java.lang.Override
        public LegacyArtifactStagingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LegacyArtifactStagingServiceBlockingStub(channel, callOptions);
        }
      };
    return LegacyArtifactStagingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static LegacyArtifactStagingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactStagingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactStagingServiceFutureStub>() {
        @java.lang.Override
        public LegacyArtifactStagingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LegacyArtifactStagingServiceFutureStub(channel, callOptions);
        }
      };
    return LegacyArtifactStagingServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * A service to stage artifacts for use in a Job.
   * </pre>
   */
  public static abstract class LegacyArtifactStagingServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Stage an artifact to be available during job execution. The first request must contain the
     * name of the artifact. All future requests must contain sequential chunks of the content of
     * the artifact.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest> putArtifact(
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getPutArtifactMethod(), responseObserver);
    }

    /**
     * <pre>
     * Commit the manifest for a Job. All artifacts must have been successfully uploaded
     * before this call is made.
     * Throws error INVALID_ARGUMENT if not all of the members of the manifest are present
     * </pre>
     */
    public void commitManifest(org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitManifestMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getPutArtifactMethod(),
            io.grpc.stub.ServerCalls.asyncClientStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest,
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse>(
                  this, METHODID_PUT_ARTIFACT)))
          .addMethod(
            getCommitManifestMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest,
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse>(
                  this, METHODID_COMMIT_MANIFEST)))
          .build();
    }
  }

  /**
   * <pre>
   * A service to stage artifacts for use in a Job.
   * </pre>
   */
  public static final class LegacyArtifactStagingServiceStub extends io.grpc.stub.AbstractAsyncStub<LegacyArtifactStagingServiceStub> {
    private LegacyArtifactStagingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LegacyArtifactStagingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LegacyArtifactStagingServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Stage an artifact to be available during job execution. The first request must contain the
     * name of the artifact. All future requests must contain sequential chunks of the content of
     * the artifact.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest> putArtifact(
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncClientStreamingCall(
          getChannel().newCall(getPutArtifactMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * Commit the manifest for a Job. All artifacts must have been successfully uploaded
     * before this call is made.
     * Throws error INVALID_ARGUMENT if not all of the members of the manifest are present
     * </pre>
     */
    public void commitManifest(org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitManifestMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * A service to stage artifacts for use in a Job.
   * </pre>
   */
  public static final class LegacyArtifactStagingServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<LegacyArtifactStagingServiceBlockingStub> {
    private LegacyArtifactStagingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LegacyArtifactStagingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LegacyArtifactStagingServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Commit the manifest for a Job. All artifacts must have been successfully uploaded
     * before this call is made.
     * Throws error INVALID_ARGUMENT if not all of the members of the manifest are present
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse commitManifest(org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitManifestMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * A service to stage artifacts for use in a Job.
   * </pre>
   */
  public static final class LegacyArtifactStagingServiceFutureStub extends io.grpc.stub.AbstractFutureStub<LegacyArtifactStagingServiceFutureStub> {
    private LegacyArtifactStagingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LegacyArtifactStagingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LegacyArtifactStagingServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Commit the manifest for a Job. All artifacts must have been successfully uploaded
     * before this call is made.
     * Throws error INVALID_ARGUMENT if not all of the members of the manifest are present
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse> commitManifest(
        org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitManifestMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_COMMIT_MANIFEST = 0;
  private static final int METHODID_PUT_ARTIFACT = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final LegacyArtifactStagingServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(LegacyArtifactStagingServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_COMMIT_MANIFEST:
          serviceImpl.commitManifest((org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PUT_ARTIFACT:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.putArtifact(
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class LegacyArtifactStagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    LegacyArtifactStagingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.jobmanagement.v1.ArtifactApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("LegacyArtifactStagingService");
    }
  }

  private static final class LegacyArtifactStagingServiceFileDescriptorSupplier
      extends LegacyArtifactStagingServiceBaseDescriptorSupplier {
    LegacyArtifactStagingServiceFileDescriptorSupplier() {}
  }

  private static final class LegacyArtifactStagingServiceMethodDescriptorSupplier
      extends LegacyArtifactStagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    LegacyArtifactStagingServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (LegacyArtifactStagingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new LegacyArtifactStagingServiceFileDescriptorSupplier())
              .addMethod(getPutArtifactMethod())
              .addMethod(getCommitManifestMethod())
              .build();
        }
      }
    }
    return result;
  }
}
