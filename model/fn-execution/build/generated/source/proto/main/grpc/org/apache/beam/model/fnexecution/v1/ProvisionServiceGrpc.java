package org.apache.beam.model.fnexecution.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * A service to provide runtime provisioning information to the SDK harness
 * worker instances -- such as pipeline options, resource constraints and
 * other job metadata -- needed by an SDK harness instance to initialize.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/fn_execution/v1/beam_provision_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ProvisionServiceGrpc {

  private ProvisionServiceGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.fn_execution.v1.ProvisionService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest,
      org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse> getGetProvisionInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetProvisionInfo",
      requestType = org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest.class,
      responseType = org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest,
      org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse> getGetProvisionInfoMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest, org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse> getGetProvisionInfoMethod;
    if ((getGetProvisionInfoMethod = ProvisionServiceGrpc.getGetProvisionInfoMethod) == null) {
      synchronized (ProvisionServiceGrpc.class) {
        if ((getGetProvisionInfoMethod = ProvisionServiceGrpc.getGetProvisionInfoMethod) == null) {
          ProvisionServiceGrpc.getGetProvisionInfoMethod = getGetProvisionInfoMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest, org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetProvisionInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ProvisionServiceMethodDescriptorSupplier("GetProvisionInfo"))
              .build();
        }
      }
    }
    return getGetProvisionInfoMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ProvisionServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProvisionServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProvisionServiceStub>() {
        @java.lang.Override
        public ProvisionServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProvisionServiceStub(channel, callOptions);
        }
      };
    return ProvisionServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ProvisionServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProvisionServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProvisionServiceBlockingStub>() {
        @java.lang.Override
        public ProvisionServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProvisionServiceBlockingStub(channel, callOptions);
        }
      };
    return ProvisionServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ProvisionServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ProvisionServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ProvisionServiceFutureStub>() {
        @java.lang.Override
        public ProvisionServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ProvisionServiceFutureStub(channel, callOptions);
        }
      };
    return ProvisionServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * A service to provide runtime provisioning information to the SDK harness
   * worker instances -- such as pipeline options, resource constraints and
   * other job metadata -- needed by an SDK harness instance to initialize.
   * </pre>
   */
  public static abstract class ProvisionServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Get provision information for the SDK harness worker instance.
     * </pre>
     */
    public void getProvisionInfo(org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetProvisionInfoMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetProvisionInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest,
                org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse>(
                  this, METHODID_GET_PROVISION_INFO)))
          .build();
    }
  }

  /**
   * <pre>
   * A service to provide runtime provisioning information to the SDK harness
   * worker instances -- such as pipeline options, resource constraints and
   * other job metadata -- needed by an SDK harness instance to initialize.
   * </pre>
   */
  public static final class ProvisionServiceStub extends io.grpc.stub.AbstractAsyncStub<ProvisionServiceStub> {
    private ProvisionServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProvisionServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProvisionServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Get provision information for the SDK harness worker instance.
     * </pre>
     */
    public void getProvisionInfo(org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetProvisionInfoMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * A service to provide runtime provisioning information to the SDK harness
   * worker instances -- such as pipeline options, resource constraints and
   * other job metadata -- needed by an SDK harness instance to initialize.
   * </pre>
   */
  public static final class ProvisionServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<ProvisionServiceBlockingStub> {
    private ProvisionServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProvisionServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProvisionServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Get provision information for the SDK harness worker instance.
     * </pre>
     */
    public org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse getProvisionInfo(org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetProvisionInfoMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * A service to provide runtime provisioning information to the SDK harness
   * worker instances -- such as pipeline options, resource constraints and
   * other job metadata -- needed by an SDK harness instance to initialize.
   * </pre>
   */
  public static final class ProvisionServiceFutureStub extends io.grpc.stub.AbstractFutureStub<ProvisionServiceFutureStub> {
    private ProvisionServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ProvisionServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ProvisionServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Get provision information for the SDK harness worker instance.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse> getProvisionInfo(
        org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetProvisionInfoMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_PROVISION_INFO = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ProvisionServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ProvisionServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_PROVISION_INFO:
          serviceImpl.getProvisionInfo((org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.ProvisionApi.GetProvisionInfoResponse>) responseObserver);
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

  private static abstract class ProvisionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ProvisionServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.fnexecution.v1.ProvisionApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ProvisionService");
    }
  }

  private static final class ProvisionServiceFileDescriptorSupplier
      extends ProvisionServiceBaseDescriptorSupplier {
    ProvisionServiceFileDescriptorSupplier() {}
  }

  private static final class ProvisionServiceMethodDescriptorSupplier
      extends ProvisionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ProvisionServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ProvisionServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ProvisionServiceFileDescriptorSupplier())
              .addMethod(getGetProvisionInfoMethod())
              .build();
        }
      }
    }
    return result;
  }
}
