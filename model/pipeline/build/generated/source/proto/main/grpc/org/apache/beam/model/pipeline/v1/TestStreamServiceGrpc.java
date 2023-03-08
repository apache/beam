package org.apache.beam.model.pipeline.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/pipeline/v1/beam_runner_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class TestStreamServiceGrpc {

  private TestStreamServiceGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.pipeline.v1.TestStreamService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest,
      org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event> getEventsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Events",
      requestType = org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest.class,
      responseType = org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest,
      org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event> getEventsMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest, org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event> getEventsMethod;
    if ((getEventsMethod = TestStreamServiceGrpc.getEventsMethod) == null) {
      synchronized (TestStreamServiceGrpc.class) {
        if ((getEventsMethod = TestStreamServiceGrpc.getEventsMethod) == null) {
          TestStreamServiceGrpc.getEventsMethod = getEventsMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest, org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Events"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event.getDefaultInstance()))
              .setSchemaDescriptor(new TestStreamServiceMethodDescriptorSupplier("Events"))
              .build();
        }
      }
    }
    return getEventsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TestStreamServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TestStreamServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TestStreamServiceStub>() {
        @java.lang.Override
        public TestStreamServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TestStreamServiceStub(channel, callOptions);
        }
      };
    return TestStreamServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TestStreamServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TestStreamServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TestStreamServiceBlockingStub>() {
        @java.lang.Override
        public TestStreamServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TestStreamServiceBlockingStub(channel, callOptions);
        }
      };
    return TestStreamServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TestStreamServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TestStreamServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TestStreamServiceFutureStub>() {
        @java.lang.Override
        public TestStreamServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TestStreamServiceFutureStub(channel, callOptions);
        }
      };
    return TestStreamServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class TestStreamServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * A TestStream will request for events using this RPC.
     * </pre>
     */
    public void events(org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEventsMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getEventsMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest,
                org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event>(
                  this, METHODID_EVENTS)))
          .build();
    }
  }

  /**
   */
  public static final class TestStreamServiceStub extends io.grpc.stub.AbstractAsyncStub<TestStreamServiceStub> {
    private TestStreamServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TestStreamServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TestStreamServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * A TestStream will request for events using this RPC.
     * </pre>
     */
    public void events(org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getEventsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class TestStreamServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<TestStreamServiceBlockingStub> {
    private TestStreamServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TestStreamServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TestStreamServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * A TestStream will request for events using this RPC.
     * </pre>
     */
    public java.util.Iterator<org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event> events(
        org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getEventsMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class TestStreamServiceFutureStub extends io.grpc.stub.AbstractFutureStub<TestStreamServiceFutureStub> {
    private TestStreamServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TestStreamServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TestStreamServiceFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_EVENTS = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TestStreamServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(TestStreamServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_EVENTS:
          serviceImpl.events((org.apache.beam.model.pipeline.v1.RunnerApi.EventsRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload.Event>) responseObserver);
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

  private static abstract class TestStreamServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TestStreamServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.pipeline.v1.RunnerApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TestStreamService");
    }
  }

  private static final class TestStreamServiceFileDescriptorSupplier
      extends TestStreamServiceBaseDescriptorSupplier {
    TestStreamServiceFileDescriptorSupplier() {}
  }

  private static final class TestStreamServiceMethodDescriptorSupplier
      extends TestStreamServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TestStreamServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (TestStreamServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TestStreamServiceFileDescriptorSupplier())
              .addMethod(getEventsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
