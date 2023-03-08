package org.apache.beam.model.jobmanagement.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * A service to retrieve artifacts for use in a Job.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/job_management/v1/beam_artifact_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class LegacyArtifactRetrievalServiceGrpc {

  private LegacyArtifactRetrievalServiceGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.job_management.v1.LegacyArtifactRetrievalService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse> getGetManifestMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetManifest",
      requestType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse> getGetManifestMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse> getGetManifestMethod;
    if ((getGetManifestMethod = LegacyArtifactRetrievalServiceGrpc.getGetManifestMethod) == null) {
      synchronized (LegacyArtifactRetrievalServiceGrpc.class) {
        if ((getGetManifestMethod = LegacyArtifactRetrievalServiceGrpc.getGetManifestMethod) == null) {
          LegacyArtifactRetrievalServiceGrpc.getGetManifestMethod = getGetManifestMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetManifest"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse.getDefaultInstance()))
              .setSchemaDescriptor(new LegacyArtifactRetrievalServiceMethodDescriptorSupplier("GetManifest"))
              .build();
        }
      }
    }
    return getGetManifestMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk> getGetArtifactMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetArtifact",
      requestType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk> getGetArtifactMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk> getGetArtifactMethod;
    if ((getGetArtifactMethod = LegacyArtifactRetrievalServiceGrpc.getGetArtifactMethod) == null) {
      synchronized (LegacyArtifactRetrievalServiceGrpc.class) {
        if ((getGetArtifactMethod = LegacyArtifactRetrievalServiceGrpc.getGetArtifactMethod) == null) {
          LegacyArtifactRetrievalServiceGrpc.getGetArtifactMethod = getGetArtifactMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetArtifact"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk.getDefaultInstance()))
              .setSchemaDescriptor(new LegacyArtifactRetrievalServiceMethodDescriptorSupplier("GetArtifact"))
              .build();
        }
      }
    }
    return getGetArtifactMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static LegacyArtifactRetrievalServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactRetrievalServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactRetrievalServiceStub>() {
        @java.lang.Override
        public LegacyArtifactRetrievalServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LegacyArtifactRetrievalServiceStub(channel, callOptions);
        }
      };
    return LegacyArtifactRetrievalServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static LegacyArtifactRetrievalServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactRetrievalServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactRetrievalServiceBlockingStub>() {
        @java.lang.Override
        public LegacyArtifactRetrievalServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LegacyArtifactRetrievalServiceBlockingStub(channel, callOptions);
        }
      };
    return LegacyArtifactRetrievalServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static LegacyArtifactRetrievalServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactRetrievalServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<LegacyArtifactRetrievalServiceFutureStub>() {
        @java.lang.Override
        public LegacyArtifactRetrievalServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new LegacyArtifactRetrievalServiceFutureStub(channel, callOptions);
        }
      };
    return LegacyArtifactRetrievalServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * A service to retrieve artifacts for use in a Job.
   * </pre>
   */
  public static abstract class LegacyArtifactRetrievalServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Get the manifest for the job
     * </pre>
     */
    public void getManifest(org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetManifestMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get an artifact staged for the job. The requested artifact must be within the manifest
     * </pre>
     */
    public void getArtifact(org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetArtifactMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetManifestMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest,
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse>(
                  this, METHODID_GET_MANIFEST)))
          .addMethod(
            getGetArtifactMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest,
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk>(
                  this, METHODID_GET_ARTIFACT)))
          .build();
    }
  }

  /**
   * <pre>
   * A service to retrieve artifacts for use in a Job.
   * </pre>
   */
  public static final class LegacyArtifactRetrievalServiceStub extends io.grpc.stub.AbstractAsyncStub<LegacyArtifactRetrievalServiceStub> {
    private LegacyArtifactRetrievalServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LegacyArtifactRetrievalServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LegacyArtifactRetrievalServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Get the manifest for the job
     * </pre>
     */
    public void getManifest(org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetManifestMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get an artifact staged for the job. The requested artifact must be within the manifest
     * </pre>
     */
    public void getArtifact(org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getGetArtifactMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * A service to retrieve artifacts for use in a Job.
   * </pre>
   */
  public static final class LegacyArtifactRetrievalServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<LegacyArtifactRetrievalServiceBlockingStub> {
    private LegacyArtifactRetrievalServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LegacyArtifactRetrievalServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LegacyArtifactRetrievalServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Get the manifest for the job
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse getManifest(org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetManifestMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get an artifact staged for the job. The requested artifact must be within the manifest
     * </pre>
     */
    public java.util.Iterator<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk> getArtifact(
        org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getGetArtifactMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * A service to retrieve artifacts for use in a Job.
   * </pre>
   */
  public static final class LegacyArtifactRetrievalServiceFutureStub extends io.grpc.stub.AbstractFutureStub<LegacyArtifactRetrievalServiceFutureStub> {
    private LegacyArtifactRetrievalServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LegacyArtifactRetrievalServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new LegacyArtifactRetrievalServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Get the manifest for the job
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse> getManifest(
        org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetManifestMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_MANIFEST = 0;
  private static final int METHODID_GET_ARTIFACT = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final LegacyArtifactRetrievalServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(LegacyArtifactRetrievalServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_MANIFEST:
          serviceImpl.getManifest((org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse>) responseObserver);
          break;
        case METHODID_GET_ARTIFACT:
          serviceImpl.getArtifact((org.apache.beam.model.jobmanagement.v1.ArtifactApi.LegacyGetArtifactRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk>) responseObserver);
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
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class LegacyArtifactRetrievalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    LegacyArtifactRetrievalServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.jobmanagement.v1.ArtifactApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("LegacyArtifactRetrievalService");
    }
  }

  private static final class LegacyArtifactRetrievalServiceFileDescriptorSupplier
      extends LegacyArtifactRetrievalServiceBaseDescriptorSupplier {
    LegacyArtifactRetrievalServiceFileDescriptorSupplier() {}
  }

  private static final class LegacyArtifactRetrievalServiceMethodDescriptorSupplier
      extends LegacyArtifactRetrievalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    LegacyArtifactRetrievalServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (LegacyArtifactRetrievalServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new LegacyArtifactRetrievalServiceFileDescriptorSupplier())
              .addMethod(getGetManifestMethod())
              .addMethod(getGetArtifactMethod())
              .build();
        }
      }
    }
    return result;
  }
}
