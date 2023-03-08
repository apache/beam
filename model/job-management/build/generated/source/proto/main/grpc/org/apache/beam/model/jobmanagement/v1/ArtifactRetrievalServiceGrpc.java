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
public final class ArtifactRetrievalServiceGrpc {

  private ArtifactRetrievalServiceGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.job_management.v1.ArtifactRetrievalService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse> getResolveArtifactsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResolveArtifacts",
      requestType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse> getResolveArtifactsMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse> getResolveArtifactsMethod;
    if ((getResolveArtifactsMethod = ArtifactRetrievalServiceGrpc.getResolveArtifactsMethod) == null) {
      synchronized (ArtifactRetrievalServiceGrpc.class) {
        if ((getResolveArtifactsMethod = ArtifactRetrievalServiceGrpc.getResolveArtifactsMethod) == null) {
          ArtifactRetrievalServiceGrpc.getResolveArtifactsMethod = getResolveArtifactsMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResolveArtifacts"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ArtifactRetrievalServiceMethodDescriptorSupplier("ResolveArtifacts"))
              .build();
        }
      }
    }
    return getResolveArtifactsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse> getGetArtifactMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetArtifact",
      requestType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse> getGetArtifactMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse> getGetArtifactMethod;
    if ((getGetArtifactMethod = ArtifactRetrievalServiceGrpc.getGetArtifactMethod) == null) {
      synchronized (ArtifactRetrievalServiceGrpc.class) {
        if ((getGetArtifactMethod = ArtifactRetrievalServiceGrpc.getGetArtifactMethod) == null) {
          ArtifactRetrievalServiceGrpc.getGetArtifactMethod = getGetArtifactMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest, org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetArtifact"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ArtifactRetrievalServiceMethodDescriptorSupplier("GetArtifact"))
              .build();
        }
      }
    }
    return getGetArtifactMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ArtifactRetrievalServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArtifactRetrievalServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArtifactRetrievalServiceStub>() {
        @java.lang.Override
        public ArtifactRetrievalServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArtifactRetrievalServiceStub(channel, callOptions);
        }
      };
    return ArtifactRetrievalServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ArtifactRetrievalServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArtifactRetrievalServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArtifactRetrievalServiceBlockingStub>() {
        @java.lang.Override
        public ArtifactRetrievalServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArtifactRetrievalServiceBlockingStub(channel, callOptions);
        }
      };
    return ArtifactRetrievalServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ArtifactRetrievalServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArtifactRetrievalServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArtifactRetrievalServiceFutureStub>() {
        @java.lang.Override
        public ArtifactRetrievalServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArtifactRetrievalServiceFutureStub(channel, callOptions);
        }
      };
    return ArtifactRetrievalServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * A service to retrieve artifacts for use in a Job.
   * </pre>
   */
  public static abstract class ArtifactRetrievalServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Resolves the given artifact references into one or more replacement
     * artifact references (e.g. a Maven dependency into a (transitive) set
     * of jars.
     * </pre>
     */
    public void resolveArtifacts(org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResolveArtifactsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Retrieves the given artifact as a stream of bytes.
     * </pre>
     */
    public void getArtifact(org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetArtifactMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getResolveArtifactsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest,
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse>(
                  this, METHODID_RESOLVE_ARTIFACTS)))
          .addMethod(
            getGetArtifactMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest,
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse>(
                  this, METHODID_GET_ARTIFACT)))
          .build();
    }
  }

  /**
   * <pre>
   * A service to retrieve artifacts for use in a Job.
   * </pre>
   */
  public static final class ArtifactRetrievalServiceStub extends io.grpc.stub.AbstractAsyncStub<ArtifactRetrievalServiceStub> {
    private ArtifactRetrievalServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArtifactRetrievalServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArtifactRetrievalServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Resolves the given artifact references into one or more replacement
     * artifact references (e.g. a Maven dependency into a (transitive) set
     * of jars.
     * </pre>
     */
    public void resolveArtifacts(org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResolveArtifactsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Retrieves the given artifact as a stream of bytes.
     * </pre>
     */
    public void getArtifact(org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getGetArtifactMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * A service to retrieve artifacts for use in a Job.
   * </pre>
   */
  public static final class ArtifactRetrievalServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<ArtifactRetrievalServiceBlockingStub> {
    private ArtifactRetrievalServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArtifactRetrievalServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArtifactRetrievalServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Resolves the given artifact references into one or more replacement
     * artifact references (e.g. a Maven dependency into a (transitive) set
     * of jars.
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse resolveArtifacts(org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResolveArtifactsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Retrieves the given artifact as a stream of bytes.
     * </pre>
     */
    public java.util.Iterator<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse> getArtifact(
        org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getGetArtifactMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * A service to retrieve artifacts for use in a Job.
   * </pre>
   */
  public static final class ArtifactRetrievalServiceFutureStub extends io.grpc.stub.AbstractFutureStub<ArtifactRetrievalServiceFutureStub> {
    private ArtifactRetrievalServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArtifactRetrievalServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArtifactRetrievalServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Resolves the given artifact references into one or more replacement
     * artifact references (e.g. a Maven dependency into a (transitive) set
     * of jars.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse> resolveArtifacts(
        org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResolveArtifactsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_RESOLVE_ARTIFACTS = 0;
  private static final int METHODID_GET_ARTIFACT = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ArtifactRetrievalServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ArtifactRetrievalServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_RESOLVE_ARTIFACTS:
          serviceImpl.resolveArtifacts((org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse>) responseObserver);
          break;
        case METHODID_GET_ARTIFACT:
          serviceImpl.getArtifact((org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse>) responseObserver);
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

  private static abstract class ArtifactRetrievalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ArtifactRetrievalServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.jobmanagement.v1.ArtifactApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ArtifactRetrievalService");
    }
  }

  private static final class ArtifactRetrievalServiceFileDescriptorSupplier
      extends ArtifactRetrievalServiceBaseDescriptorSupplier {
    ArtifactRetrievalServiceFileDescriptorSupplier() {}
  }

  private static final class ArtifactRetrievalServiceMethodDescriptorSupplier
      extends ArtifactRetrievalServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ArtifactRetrievalServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ArtifactRetrievalServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ArtifactRetrievalServiceFileDescriptorSupplier())
              .addMethod(getResolveArtifactsMethod())
              .addMethod(getGetArtifactMethod())
              .build();
        }
      }
    }
    return result;
  }
}
