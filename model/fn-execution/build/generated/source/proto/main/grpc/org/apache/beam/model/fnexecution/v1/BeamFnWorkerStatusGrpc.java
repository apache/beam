package org.apache.beam.model.fnexecution.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * API for SDKs to report debug-related statuses to runner during pipeline execution.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/fn_execution/v1/beam_fn_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class BeamFnWorkerStatusGrpc {

  private BeamFnWorkerStatusGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.fn_execution.v1.BeamFnWorkerStatus";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest> getWorkerStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WorkerStatus",
      requestType = org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse.class,
      responseType = org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest> getWorkerStatusMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse, org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest> getWorkerStatusMethod;
    if ((getWorkerStatusMethod = BeamFnWorkerStatusGrpc.getWorkerStatusMethod) == null) {
      synchronized (BeamFnWorkerStatusGrpc.class) {
        if ((getWorkerStatusMethod = BeamFnWorkerStatusGrpc.getWorkerStatusMethod) == null) {
          BeamFnWorkerStatusGrpc.getWorkerStatusMethod = getWorkerStatusMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse, org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WorkerStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest.getDefaultInstance()))
              .setSchemaDescriptor(new BeamFnWorkerStatusMethodDescriptorSupplier("WorkerStatus"))
              .build();
        }
      }
    }
    return getWorkerStatusMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BeamFnWorkerStatusStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnWorkerStatusStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnWorkerStatusStub>() {
        @java.lang.Override
        public BeamFnWorkerStatusStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnWorkerStatusStub(channel, callOptions);
        }
      };
    return BeamFnWorkerStatusStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BeamFnWorkerStatusBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnWorkerStatusBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnWorkerStatusBlockingStub>() {
        @java.lang.Override
        public BeamFnWorkerStatusBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnWorkerStatusBlockingStub(channel, callOptions);
        }
      };
    return BeamFnWorkerStatusBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BeamFnWorkerStatusFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnWorkerStatusFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnWorkerStatusFutureStub>() {
        @java.lang.Override
        public BeamFnWorkerStatusFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnWorkerStatusFutureStub(channel, callOptions);
        }
      };
    return BeamFnWorkerStatusFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * API for SDKs to report debug-related statuses to runner during pipeline execution.
   * </pre>
   */
  public static abstract class BeamFnWorkerStatusImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse> workerStatus(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getWorkerStatusMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getWorkerStatusMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse,
                org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest>(
                  this, METHODID_WORKER_STATUS)))
          .build();
    }
  }

  /**
   * <pre>
   * API for SDKs to report debug-related statuses to runner during pipeline execution.
   * </pre>
   */
  public static final class BeamFnWorkerStatusStub extends io.grpc.stub.AbstractAsyncStub<BeamFnWorkerStatusStub> {
    private BeamFnWorkerStatusStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnWorkerStatusStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnWorkerStatusStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse> workerStatus(
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getWorkerStatusMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * <pre>
   * API for SDKs to report debug-related statuses to runner during pipeline execution.
   * </pre>
   */
  public static final class BeamFnWorkerStatusBlockingStub extends io.grpc.stub.AbstractBlockingStub<BeamFnWorkerStatusBlockingStub> {
    private BeamFnWorkerStatusBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnWorkerStatusBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnWorkerStatusBlockingStub(channel, callOptions);
    }
  }

  /**
   * <pre>
   * API for SDKs to report debug-related statuses to runner during pipeline execution.
   * </pre>
   */
  public static final class BeamFnWorkerStatusFutureStub extends io.grpc.stub.AbstractFutureStub<BeamFnWorkerStatusFutureStub> {
    private BeamFnWorkerStatusFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnWorkerStatusFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnWorkerStatusFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_WORKER_STATUS = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BeamFnWorkerStatusImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BeamFnWorkerStatusImplBase serviceImpl, int methodId) {
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
        case METHODID_WORKER_STATUS:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.workerStatus(
              (io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class BeamFnWorkerStatusBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BeamFnWorkerStatusBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.fnexecution.v1.BeamFnApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BeamFnWorkerStatus");
    }
  }

  private static final class BeamFnWorkerStatusFileDescriptorSupplier
      extends BeamFnWorkerStatusBaseDescriptorSupplier {
    BeamFnWorkerStatusFileDescriptorSupplier() {}
  }

  private static final class BeamFnWorkerStatusMethodDescriptorSupplier
      extends BeamFnWorkerStatusBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BeamFnWorkerStatusMethodDescriptorSupplier(String methodName) {
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
      synchronized (BeamFnWorkerStatusGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BeamFnWorkerStatusFileDescriptorSupplier())
              .addMethod(getWorkerStatusMethod())
              .build();
        }
      }
    }
    return result;
  }
}
