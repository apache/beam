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
public final class BeamFnDataGrpc {

  private BeamFnDataGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.fn_execution.v1.BeamFnData";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements> getDataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Data",
      requestType = org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.class,
      responseType = org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements> getDataMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements, org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements> getDataMethod;
    if ((getDataMethod = BeamFnDataGrpc.getDataMethod) == null) {
      synchronized (BeamFnDataGrpc.class) {
        if ((getDataMethod = BeamFnDataGrpc.getDataMethod) == null) {
          BeamFnDataGrpc.getDataMethod = getDataMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements, org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Data"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.getDefaultInstance()))
              .setSchemaDescriptor(new BeamFnDataMethodDescriptorSupplier("Data"))
              .build();
        }
      }
    }
    return getDataMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BeamFnDataStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnDataStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnDataStub>() {
        @java.lang.Override
        public BeamFnDataStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnDataStub(channel, callOptions);
        }
      };
    return BeamFnDataStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BeamFnDataBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnDataBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnDataBlockingStub>() {
        @java.lang.Override
        public BeamFnDataBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnDataBlockingStub(channel, callOptions);
        }
      };
    return BeamFnDataBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BeamFnDataFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnDataFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnDataFutureStub>() {
        @java.lang.Override
        public BeamFnDataFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnDataFutureStub(channel, callOptions);
        }
      };
    return BeamFnDataFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Stable
   * </pre>
   */
  public static abstract class BeamFnDataImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Used to send data between harnesses.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements> data(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getDataMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getDataMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements,
                org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements>(
                  this, METHODID_DATA)))
          .build();
    }
  }

  /**
   * <pre>
   * Stable
   * </pre>
   */
  public static final class BeamFnDataStub extends io.grpc.stub.AbstractAsyncStub<BeamFnDataStub> {
    private BeamFnDataStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnDataStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnDataStub(channel, callOptions);
    }

    /**
     * <pre>
     * Used to send data between harnesses.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements> data(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getDataMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * <pre>
   * Stable
   * </pre>
   */
  public static final class BeamFnDataBlockingStub extends io.grpc.stub.AbstractBlockingStub<BeamFnDataBlockingStub> {
    private BeamFnDataBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnDataBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnDataBlockingStub(channel, callOptions);
    }
  }

  /**
   * <pre>
   * Stable
   * </pre>
   */
  public static final class BeamFnDataFutureStub extends io.grpc.stub.AbstractFutureStub<BeamFnDataFutureStub> {
    private BeamFnDataFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnDataFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnDataFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_DATA = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BeamFnDataImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BeamFnDataImplBase serviceImpl, int methodId) {
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
        case METHODID_DATA:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.data(
              (io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BeamFnDataBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BeamFnDataBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.fnexecution.v1.BeamFnApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BeamFnData");
    }
  }

  private static final class BeamFnDataFileDescriptorSupplier
      extends BeamFnDataBaseDescriptorSupplier {
    BeamFnDataFileDescriptorSupplier() {}
  }

  private static final class BeamFnDataMethodDescriptorSupplier
      extends BeamFnDataBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BeamFnDataMethodDescriptorSupplier(String methodName) {
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
      synchronized (BeamFnDataGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BeamFnDataFileDescriptorSupplier())
              .addMethod(getDataMethod())
              .build();
        }
      }
    }
    return result;
  }
}
