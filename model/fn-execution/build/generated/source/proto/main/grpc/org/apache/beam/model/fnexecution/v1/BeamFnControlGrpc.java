package org.apache.beam.model.fnexecution.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * An API that describes the work that a SDK harness is meant to do.
 * Stable
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/fn_execution/v1/beam_fn_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class BeamFnControlGrpc {

  private BeamFnControlGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.fn_execution.v1.BeamFnControl";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest> getControlMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Control",
      requestType = org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse.class,
      responseType = org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest> getControlMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse, org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest> getControlMethod;
    if ((getControlMethod = BeamFnControlGrpc.getControlMethod) == null) {
      synchronized (BeamFnControlGrpc.class) {
        if ((getControlMethod = BeamFnControlGrpc.getControlMethod) == null) {
          BeamFnControlGrpc.getControlMethod = getControlMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse, org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Control"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest.getDefaultInstance()))
              .setSchemaDescriptor(new BeamFnControlMethodDescriptorSupplier("Control"))
              .build();
        }
      }
    }
    return getControlMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor> getGetProcessBundleDescriptorMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetProcessBundleDescriptor",
      requestType = org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest.class,
      responseType = org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor> getGetProcessBundleDescriptorMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest, org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor> getGetProcessBundleDescriptorMethod;
    if ((getGetProcessBundleDescriptorMethod = BeamFnControlGrpc.getGetProcessBundleDescriptorMethod) == null) {
      synchronized (BeamFnControlGrpc.class) {
        if ((getGetProcessBundleDescriptorMethod = BeamFnControlGrpc.getGetProcessBundleDescriptorMethod) == null) {
          BeamFnControlGrpc.getGetProcessBundleDescriptorMethod = getGetProcessBundleDescriptorMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest, org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetProcessBundleDescriptor"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor.getDefaultInstance()))
              .setSchemaDescriptor(new BeamFnControlMethodDescriptorSupplier("GetProcessBundleDescriptor"))
              .build();
        }
      }
    }
    return getGetProcessBundleDescriptorMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BeamFnControlStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnControlStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnControlStub>() {
        @java.lang.Override
        public BeamFnControlStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnControlStub(channel, callOptions);
        }
      };
    return BeamFnControlStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BeamFnControlBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnControlBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnControlBlockingStub>() {
        @java.lang.Override
        public BeamFnControlBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnControlBlockingStub(channel, callOptions);
        }
      };
    return BeamFnControlBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BeamFnControlFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnControlFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnControlFutureStub>() {
        @java.lang.Override
        public BeamFnControlFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnControlFutureStub(channel, callOptions);
        }
      };
    return BeamFnControlFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * An API that describes the work that a SDK harness is meant to do.
   * Stable
   * </pre>
   */
  public static abstract class BeamFnControlImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Instructions sent by the runner to the SDK requesting different types
     * of work.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse> control(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getControlMethod(), responseObserver);
    }

    /**
     * <pre>
     * Used to get the full process bundle descriptors for bundles one
     * is asked to process.
     * </pre>
     */
    public void getProcessBundleDescriptor(org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetProcessBundleDescriptorMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getControlMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse,
                org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest>(
                  this, METHODID_CONTROL)))
          .addMethod(
            getGetProcessBundleDescriptorMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest,
                org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor>(
                  this, METHODID_GET_PROCESS_BUNDLE_DESCRIPTOR)))
          .build();
    }
  }

  /**
   * <pre>
   * An API that describes the work that a SDK harness is meant to do.
   * Stable
   * </pre>
   */
  public static final class BeamFnControlStub extends io.grpc.stub.AbstractAsyncStub<BeamFnControlStub> {
    private BeamFnControlStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnControlStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnControlStub(channel, callOptions);
    }

    /**
     * <pre>
     * Instructions sent by the runner to the SDK requesting different types
     * of work.
     * </pre>
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse> control(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getControlMethod(), getCallOptions()), responseObserver);
    }

    /**
     * <pre>
     * Used to get the full process bundle descriptors for bundles one
     * is asked to process.
     * </pre>
     */
    public void getProcessBundleDescriptor(org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetProcessBundleDescriptorMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * An API that describes the work that a SDK harness is meant to do.
   * Stable
   * </pre>
   */
  public static final class BeamFnControlBlockingStub extends io.grpc.stub.AbstractBlockingStub<BeamFnControlBlockingStub> {
    private BeamFnControlBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnControlBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnControlBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Used to get the full process bundle descriptors for bundles one
     * is asked to process.
     * </pre>
     */
    public org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor getProcessBundleDescriptor(org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetProcessBundleDescriptorMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * An API that describes the work that a SDK harness is meant to do.
   * Stable
   * </pre>
   */
  public static final class BeamFnControlFutureStub extends io.grpc.stub.AbstractFutureStub<BeamFnControlFutureStub> {
    private BeamFnControlFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnControlFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnControlFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Used to get the full process bundle descriptors for bundles one
     * is asked to process.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor> getProcessBundleDescriptor(
        org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetProcessBundleDescriptorMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_PROCESS_BUNDLE_DESCRIPTOR = 0;
  private static final int METHODID_CONTROL = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BeamFnControlImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BeamFnControlImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_PROCESS_BUNDLE_DESCRIPTOR:
          serviceImpl.getProcessBundleDescriptor((org.apache.beam.model.fnexecution.v1.BeamFnApi.GetProcessBundleDescriptorRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor>) responseObserver);
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
        case METHODID_CONTROL:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.control(
              (io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BeamFnControlBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BeamFnControlBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.fnexecution.v1.BeamFnApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BeamFnControl");
    }
  }

  private static final class BeamFnControlFileDescriptorSupplier
      extends BeamFnControlBaseDescriptorSupplier {
    BeamFnControlFileDescriptorSupplier() {}
  }

  private static final class BeamFnControlMethodDescriptorSupplier
      extends BeamFnControlBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BeamFnControlMethodDescriptorSupplier(String methodName) {
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
      synchronized (BeamFnControlGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BeamFnControlFileDescriptorSupplier())
              .addMethod(getControlMethod())
              .addMethod(getGetProcessBundleDescriptorMethod())
              .build();
        }
      }
    }
    return result;
  }
}
