package org.apache.beam.model.fnexecution.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Stable
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/fn_execution/v1/beam_fn_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class BeamFnLoggingGrpc {

  private BeamFnLoggingGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.fn_execution.v1.BeamFnLogging";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.List,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl> getLoggingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Logging",
      requestType = org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.List.class,
      responseType = org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.List,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl> getLoggingMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.List, org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl> getLoggingMethod;
    if ((getLoggingMethod = BeamFnLoggingGrpc.getLoggingMethod) == null) {
      synchronized (BeamFnLoggingGrpc.class) {
        if ((getLoggingMethod = BeamFnLoggingGrpc.getLoggingMethod) == null) {
          BeamFnLoggingGrpc.getLoggingMethod = getLoggingMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.List, org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Logging"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.List.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl.getDefaultInstance()))
              .setSchemaDescriptor(new BeamFnLoggingMethodDescriptorSupplier("Logging"))
              .build();
        }
      }
    }
    return getLoggingMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BeamFnLoggingStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnLoggingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnLoggingStub>() {
        @java.lang.Override
        public BeamFnLoggingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnLoggingStub(channel, callOptions);
        }
      };
    return BeamFnLoggingStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BeamFnLoggingBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnLoggingBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnLoggingBlockingStub>() {
        @java.lang.Override
        public BeamFnLoggingBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnLoggingBlockingStub(channel, callOptions);
        }
      };
    return BeamFnLoggingBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BeamFnLoggingFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnLoggingFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnLoggingFutureStub>() {
        @java.lang.Override
        public BeamFnLoggingFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnLoggingFutureStub(channel, callOptions);
        }
      };
    return BeamFnLoggingFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Stable
   * </pre>
   */
  public static abstract class BeamFnLoggingImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Allows for the SDK to emit log entries which the runner can
     * associate with the active job.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.List> logging(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getLoggingMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getLoggingMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.List,
                org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl>(
                  this, METHODID_LOGGING)))
          .build();
    }
  }

  /**
   * <pre>
   * Stable
   * </pre>
   */
  public static final class BeamFnLoggingStub extends io.grpc.stub.AbstractAsyncStub<BeamFnLoggingStub> {
    private BeamFnLoggingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnLoggingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnLoggingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Allows for the SDK to emit log entries which the runner can
     * associate with the active job.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.List> logging(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getLoggingMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * <pre>
   * Stable
   * </pre>
   */
  public static final class BeamFnLoggingBlockingStub extends io.grpc.stub.AbstractBlockingStub<BeamFnLoggingBlockingStub> {
    private BeamFnLoggingBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnLoggingBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnLoggingBlockingStub(channel, callOptions);
    }
  }

  /**
   * <pre>
   * Stable
   * </pre>
   */
  public static final class BeamFnLoggingFutureStub extends io.grpc.stub.AbstractFutureStub<BeamFnLoggingFutureStub> {
    private BeamFnLoggingFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnLoggingFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnLoggingFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_LOGGING = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BeamFnLoggingImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BeamFnLoggingImplBase serviceImpl, int methodId) {
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
        case METHODID_LOGGING:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.logging(
              (io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BeamFnLoggingBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BeamFnLoggingBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.fnexecution.v1.BeamFnApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BeamFnLogging");
    }
  }

  private static final class BeamFnLoggingFileDescriptorSupplier
      extends BeamFnLoggingBaseDescriptorSupplier {
    BeamFnLoggingFileDescriptorSupplier() {}
  }

  private static final class BeamFnLoggingMethodDescriptorSupplier
      extends BeamFnLoggingBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BeamFnLoggingMethodDescriptorSupplier(String methodName) {
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
      synchronized (BeamFnLoggingGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BeamFnLoggingFileDescriptorSupplier())
              .addMethod(getLoggingMethod())
              .build();
        }
      }
    }
    return result;
  }
}
