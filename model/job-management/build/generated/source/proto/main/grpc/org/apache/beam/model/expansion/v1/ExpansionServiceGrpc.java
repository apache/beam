package org.apache.beam.model.expansion.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Job Service for constructing pipelines
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/job_management/v1/beam_expansion_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ExpansionServiceGrpc {

  private ExpansionServiceGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.expansion.v1.ExpansionService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest,
      org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse> getExpandMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Expand",
      requestType = org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest.class,
      responseType = org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest,
      org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse> getExpandMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest, org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse> getExpandMethod;
    if ((getExpandMethod = ExpansionServiceGrpc.getExpandMethod) == null) {
      synchronized (ExpansionServiceGrpc.class) {
        if ((getExpandMethod = ExpansionServiceGrpc.getExpandMethod) == null) {
          ExpansionServiceGrpc.getExpandMethod = getExpandMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest, org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Expand"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ExpansionServiceMethodDescriptorSupplier("Expand"))
              .build();
        }
      }
    }
    return getExpandMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest,
      org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse> getDiscoverSchemaTransformMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DiscoverSchemaTransform",
      requestType = org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest.class,
      responseType = org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest,
      org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse> getDiscoverSchemaTransformMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest, org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse> getDiscoverSchemaTransformMethod;
    if ((getDiscoverSchemaTransformMethod = ExpansionServiceGrpc.getDiscoverSchemaTransformMethod) == null) {
      synchronized (ExpansionServiceGrpc.class) {
        if ((getDiscoverSchemaTransformMethod = ExpansionServiceGrpc.getDiscoverSchemaTransformMethod) == null) {
          ExpansionServiceGrpc.getDiscoverSchemaTransformMethod = getDiscoverSchemaTransformMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest, org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DiscoverSchemaTransform"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ExpansionServiceMethodDescriptorSupplier("DiscoverSchemaTransform"))
              .build();
        }
      }
    }
    return getDiscoverSchemaTransformMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ExpansionServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ExpansionServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ExpansionServiceStub>() {
        @java.lang.Override
        public ExpansionServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ExpansionServiceStub(channel, callOptions);
        }
      };
    return ExpansionServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ExpansionServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ExpansionServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ExpansionServiceBlockingStub>() {
        @java.lang.Override
        public ExpansionServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ExpansionServiceBlockingStub(channel, callOptions);
        }
      };
    return ExpansionServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ExpansionServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ExpansionServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ExpansionServiceFutureStub>() {
        @java.lang.Override
        public ExpansionServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ExpansionServiceFutureStub(channel, callOptions);
        }
      };
    return ExpansionServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Job Service for constructing pipelines
   * </pre>
   */
  public static abstract class ExpansionServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void expand(org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExpandMethod(), responseObserver);
    }

    /**
     * <pre>
     *A RPC to discover already registered SchemaTransformProviders.
     * See https://s.apache.org/easy-multi-language for more details.
     * </pre>
     */
    public void discoverSchemaTransform(org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDiscoverSchemaTransformMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getExpandMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest,
                org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse>(
                  this, METHODID_EXPAND)))
          .addMethod(
            getDiscoverSchemaTransformMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest,
                org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse>(
                  this, METHODID_DISCOVER_SCHEMA_TRANSFORM)))
          .build();
    }
  }

  /**
   * <pre>
   * Job Service for constructing pipelines
   * </pre>
   */
  public static final class ExpansionServiceStub extends io.grpc.stub.AbstractAsyncStub<ExpansionServiceStub> {
    private ExpansionServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ExpansionServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ExpansionServiceStub(channel, callOptions);
    }

    /**
     */
    public void expand(org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExpandMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     *A RPC to discover already registered SchemaTransformProviders.
     * See https://s.apache.org/easy-multi-language for more details.
     * </pre>
     */
    public void discoverSchemaTransform(org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDiscoverSchemaTransformMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Job Service for constructing pipelines
   * </pre>
   */
  public static final class ExpansionServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<ExpansionServiceBlockingStub> {
    private ExpansionServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ExpansionServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ExpansionServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse expand(org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExpandMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     *A RPC to discover already registered SchemaTransformProviders.
     * See https://s.apache.org/easy-multi-language for more details.
     * </pre>
     */
    public org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse discoverSchemaTransform(org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDiscoverSchemaTransformMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Job Service for constructing pipelines
   * </pre>
   */
  public static final class ExpansionServiceFutureStub extends io.grpc.stub.AbstractFutureStub<ExpansionServiceFutureStub> {
    private ExpansionServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ExpansionServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ExpansionServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse> expand(
        org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExpandMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     *A RPC to discover already registered SchemaTransformProviders.
     * See https://s.apache.org/easy-multi-language for more details.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse> discoverSchemaTransform(
        org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDiscoverSchemaTransformMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_EXPAND = 0;
  private static final int METHODID_DISCOVER_SCHEMA_TRANSFORM = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ExpansionServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ExpansionServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_EXPAND:
          serviceImpl.expand((org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse>) responseObserver);
          break;
        case METHODID_DISCOVER_SCHEMA_TRANSFORM:
          serviceImpl.discoverSchemaTransform((org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse>) responseObserver);
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

  private static abstract class ExpansionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ExpansionServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.expansion.v1.ExpansionApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ExpansionService");
    }
  }

  private static final class ExpansionServiceFileDescriptorSupplier
      extends ExpansionServiceBaseDescriptorSupplier {
    ExpansionServiceFileDescriptorSupplier() {}
  }

  private static final class ExpansionServiceMethodDescriptorSupplier
      extends ExpansionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ExpansionServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ExpansionServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ExpansionServiceFileDescriptorSupplier())
              .addMethod(getExpandMethod())
              .addMethod(getDiscoverSchemaTransformMethod())
              .build();
        }
      }
    }
    return result;
  }
}
