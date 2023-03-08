package org.apache.beam.model.fnexecution.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/fn_execution/v1/beam_fn_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class BeamFnStateGrpc {

  private BeamFnStateGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.fn_execution.v1.BeamFnState";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse> getStateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "State",
      requestType = org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.class,
      responseType = org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse> getStateMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest, org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse> getStateMethod;
    if ((getStateMethod = BeamFnStateGrpc.getStateMethod) == null) {
      synchronized (BeamFnStateGrpc.class) {
        if ((getStateMethod = BeamFnStateGrpc.getStateMethod) == null) {
          BeamFnStateGrpc.getStateMethod = getStateMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest, org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "State"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BeamFnStateMethodDescriptorSupplier("State"))
              .build();
        }
      }
    }
    return getStateMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BeamFnStateStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnStateStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnStateStub>() {
        @java.lang.Override
        public BeamFnStateStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnStateStub(channel, callOptions);
        }
      };
    return BeamFnStateStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BeamFnStateBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnStateBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnStateBlockingStub>() {
        @java.lang.Override
        public BeamFnStateBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnStateBlockingStub(channel, callOptions);
        }
      };
    return BeamFnStateBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BeamFnStateFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnStateFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnStateFutureStub>() {
        @java.lang.Override
        public BeamFnStateFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnStateFutureStub(channel, callOptions);
        }
      };
    return BeamFnStateFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class BeamFnStateImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Used to get/append/clear state stored by the runner on behalf of the SDK.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest> state(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getStateMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getStateMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest,
                org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse>(
                  this, METHODID_STATE)))
          .build();
    }
  }

  /**
   */
  public static final class BeamFnStateStub extends io.grpc.stub.AbstractAsyncStub<BeamFnStateStub> {
    private BeamFnStateStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnStateStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnStateStub(channel, callOptions);
    }

    /**
     * <pre>
     * Used to get/append/clear state stored by the runner on behalf of the SDK.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest> state(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getStateMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   */
  public static final class BeamFnStateBlockingStub extends io.grpc.stub.AbstractBlockingStub<BeamFnStateBlockingStub> {
    private BeamFnStateBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnStateBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnStateBlockingStub(channel, callOptions);
    }
  }

  /**
   */
  public static final class BeamFnStateFutureStub extends io.grpc.stub.AbstractFutureStub<BeamFnStateFutureStub> {
    private BeamFnStateFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnStateFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnStateFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_STATE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BeamFnStateImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BeamFnStateImplBase serviceImpl, int methodId) {
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
        case METHODID_STATE:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.state(
              (io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BeamFnStateBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BeamFnStateBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.fnexecution.v1.BeamFnApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BeamFnState");
    }
  }

  private static final class BeamFnStateFileDescriptorSupplier
      extends BeamFnStateBaseDescriptorSupplier {
    BeamFnStateFileDescriptorSupplier() {}
  }

  private static final class BeamFnStateMethodDescriptorSupplier
      extends BeamFnStateBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BeamFnStateMethodDescriptorSupplier(String methodName) {
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
      synchronized (BeamFnStateGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BeamFnStateFileDescriptorSupplier())
              .addMethod(getStateMethod())
              .build();
        }
      }
    }
    return result;
  }
}
