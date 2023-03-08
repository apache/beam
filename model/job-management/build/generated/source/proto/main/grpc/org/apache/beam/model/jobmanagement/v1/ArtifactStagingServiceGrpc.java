package org.apache.beam.model.jobmanagement.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * A service that allows the client to act as an ArtifactRetrievalService,
 * for a particular job with the server initiating requests and receiving
 * responses.
 * A client calls the service with an ArtifactResponseWrapper that has the
 * staging token set, and thereafter responds to the server's requests.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/job_management/v1/beam_artifact_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ArtifactStagingServiceGrpc {

  private ArtifactStagingServiceGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.job_management.v1.ArtifactStagingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactResponseWrapper,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper> getReverseArtifactRetrievalServiceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReverseArtifactRetrievalService",
      requestType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactResponseWrapper.class,
      responseType = org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactResponseWrapper,
      org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper> getReverseArtifactRetrievalServiceMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactResponseWrapper, org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper> getReverseArtifactRetrievalServiceMethod;
    if ((getReverseArtifactRetrievalServiceMethod = ArtifactStagingServiceGrpc.getReverseArtifactRetrievalServiceMethod) == null) {
      synchronized (ArtifactStagingServiceGrpc.class) {
        if ((getReverseArtifactRetrievalServiceMethod = ArtifactStagingServiceGrpc.getReverseArtifactRetrievalServiceMethod) == null) {
          ArtifactStagingServiceGrpc.getReverseArtifactRetrievalServiceMethod = getReverseArtifactRetrievalServiceMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactResponseWrapper, org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReverseArtifactRetrievalService"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactResponseWrapper.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper.getDefaultInstance()))
              .setSchemaDescriptor(new ArtifactStagingServiceMethodDescriptorSupplier("ReverseArtifactRetrievalService"))
              .build();
        }
      }
    }
    return getReverseArtifactRetrievalServiceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ArtifactStagingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArtifactStagingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArtifactStagingServiceStub>() {
        @java.lang.Override
        public ArtifactStagingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArtifactStagingServiceStub(channel, callOptions);
        }
      };
    return ArtifactStagingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ArtifactStagingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArtifactStagingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArtifactStagingServiceBlockingStub>() {
        @java.lang.Override
        public ArtifactStagingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArtifactStagingServiceBlockingStub(channel, callOptions);
        }
      };
    return ArtifactStagingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ArtifactStagingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArtifactStagingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArtifactStagingServiceFutureStub>() {
        @java.lang.Override
        public ArtifactStagingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArtifactStagingServiceFutureStub(channel, callOptions);
        }
      };
    return ArtifactStagingServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * A service that allows the client to act as an ArtifactRetrievalService,
   * for a particular job with the server initiating requests and receiving
   * responses.
   * A client calls the service with an ArtifactResponseWrapper that has the
   * staging token set, and thereafter responds to the server's requests.
   * </pre>
   */
  public static abstract class ArtifactStagingServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactResponseWrapper> reverseArtifactRetrievalService(
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getReverseArtifactRetrievalServiceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getReverseArtifactRetrievalServiceMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactResponseWrapper,
                org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper>(
                  this, METHODID_REVERSE_ARTIFACT_RETRIEVAL_SERVICE)))
          .build();
    }
  }

  /**
   * <pre>
   * A service that allows the client to act as an ArtifactRetrievalService,
   * for a particular job with the server initiating requests and receiving
   * responses.
   * A client calls the service with an ArtifactResponseWrapper that has the
   * staging token set, and thereafter responds to the server's requests.
   * </pre>
   */
  public static final class ArtifactStagingServiceStub extends io.grpc.stub.AbstractAsyncStub<ArtifactStagingServiceStub> {
    private ArtifactStagingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArtifactStagingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArtifactStagingServiceStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactResponseWrapper> reverseArtifactRetrievalService(
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getReverseArtifactRetrievalServiceMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * <pre>
   * A service that allows the client to act as an ArtifactRetrievalService,
   * for a particular job with the server initiating requests and receiving
   * responses.
   * A client calls the service with an ArtifactResponseWrapper that has the
   * staging token set, and thereafter responds to the server's requests.
   * </pre>
   */
  public static final class ArtifactStagingServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<ArtifactStagingServiceBlockingStub> {
    private ArtifactStagingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArtifactStagingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArtifactStagingServiceBlockingStub(channel, callOptions);
    }
  }

  /**
   * <pre>
   * A service that allows the client to act as an ArtifactRetrievalService,
   * for a particular job with the server initiating requests and receiving
   * responses.
   * A client calls the service with an ArtifactResponseWrapper that has the
   * staging token set, and thereafter responds to the server's requests.
   * </pre>
   */
  public static final class ArtifactStagingServiceFutureStub extends io.grpc.stub.AbstractFutureStub<ArtifactStagingServiceFutureStub> {
    private ArtifactStagingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArtifactStagingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArtifactStagingServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_REVERSE_ARTIFACT_RETRIEVAL_SERVICE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ArtifactStagingServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ArtifactStagingServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REVERSE_ARTIFACT_RETRIEVAL_SERVICE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.reverseArtifactRetrievalService(
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactRequestWrapper>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class ArtifactStagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ArtifactStagingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.jobmanagement.v1.ArtifactApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ArtifactStagingService");
    }
  }

  private static final class ArtifactStagingServiceFileDescriptorSupplier
      extends ArtifactStagingServiceBaseDescriptorSupplier {
    ArtifactStagingServiceFileDescriptorSupplier() {}
  }

  private static final class ArtifactStagingServiceMethodDescriptorSupplier
      extends ArtifactStagingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ArtifactStagingServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ArtifactStagingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ArtifactStagingServiceFileDescriptorSupplier())
              .addMethod(getReverseArtifactRetrievalServiceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
