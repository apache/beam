package org.apache.beam.model.fnexecution.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/fn_execution/v1/beam_fn_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class BeamFnExternalWorkerPoolGrpc {

  private BeamFnExternalWorkerPoolGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.fn_execution.v1.BeamFnExternalWorkerPool";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse> getStartWorkerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartWorker",
      requestType = org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest.class,
      responseType = org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse> getStartWorkerMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest, org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse> getStartWorkerMethod;
    if ((getStartWorkerMethod = BeamFnExternalWorkerPoolGrpc.getStartWorkerMethod) == null) {
      synchronized (BeamFnExternalWorkerPoolGrpc.class) {
        if ((getStartWorkerMethod = BeamFnExternalWorkerPoolGrpc.getStartWorkerMethod) == null) {
          BeamFnExternalWorkerPoolGrpc.getStartWorkerMethod = getStartWorkerMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest, org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartWorker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BeamFnExternalWorkerPoolMethodDescriptorSupplier("StartWorker"))
              .build();
        }
      }
    }
    return getStartWorkerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse> getStopWorkerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StopWorker",
      requestType = org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest.class,
      responseType = org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest,
      org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse> getStopWorkerMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest, org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse> getStopWorkerMethod;
    if ((getStopWorkerMethod = BeamFnExternalWorkerPoolGrpc.getStopWorkerMethod) == null) {
      synchronized (BeamFnExternalWorkerPoolGrpc.class) {
        if ((getStopWorkerMethod = BeamFnExternalWorkerPoolGrpc.getStopWorkerMethod) == null) {
          BeamFnExternalWorkerPoolGrpc.getStopWorkerMethod = getStopWorkerMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest, org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StopWorker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse.getDefaultInstance()))
              .setSchemaDescriptor(new BeamFnExternalWorkerPoolMethodDescriptorSupplier("StopWorker"))
              .build();
        }
      }
    }
    return getStopWorkerMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BeamFnExternalWorkerPoolStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnExternalWorkerPoolStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnExternalWorkerPoolStub>() {
        @java.lang.Override
        public BeamFnExternalWorkerPoolStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnExternalWorkerPoolStub(channel, callOptions);
        }
      };
    return BeamFnExternalWorkerPoolStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BeamFnExternalWorkerPoolBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnExternalWorkerPoolBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnExternalWorkerPoolBlockingStub>() {
        @java.lang.Override
        public BeamFnExternalWorkerPoolBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnExternalWorkerPoolBlockingStub(channel, callOptions);
        }
      };
    return BeamFnExternalWorkerPoolBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static BeamFnExternalWorkerPoolFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<BeamFnExternalWorkerPoolFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<BeamFnExternalWorkerPoolFutureStub>() {
        @java.lang.Override
        public BeamFnExternalWorkerPoolFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new BeamFnExternalWorkerPoolFutureStub(channel, callOptions);
        }
      };
    return BeamFnExternalWorkerPoolFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class BeamFnExternalWorkerPoolImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Start the SDK worker with the given ID.
     * </pre>
     */
    public void startWorker(org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartWorkerMethod(), responseObserver);
    }

    /**
     * <pre>
     * Stop the SDK worker.
     * </pre>
     */
    public void stopWorker(org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStopWorkerMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getStartWorkerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest,
                org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse>(
                  this, METHODID_START_WORKER)))
          .addMethod(
            getStopWorkerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest,
                org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse>(
                  this, METHODID_STOP_WORKER)))
          .build();
    }
  }

  /**
   */
  public static final class BeamFnExternalWorkerPoolStub extends io.grpc.stub.AbstractAsyncStub<BeamFnExternalWorkerPoolStub> {
    private BeamFnExternalWorkerPoolStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnExternalWorkerPoolStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnExternalWorkerPoolStub(channel, callOptions);
    }

    /**
     * <pre>
     * Start the SDK worker with the given ID.
     * </pre>
     */
    public void startWorker(org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartWorkerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Stop the SDK worker.
     * </pre>
     */
    public void stopWorker(org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStopWorkerMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class BeamFnExternalWorkerPoolBlockingStub extends io.grpc.stub.AbstractBlockingStub<BeamFnExternalWorkerPoolBlockingStub> {
    private BeamFnExternalWorkerPoolBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnExternalWorkerPoolBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnExternalWorkerPoolBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Start the SDK worker with the given ID.
     * </pre>
     */
    public org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse startWorker(org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartWorkerMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Stop the SDK worker.
     * </pre>
     */
    public org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse stopWorker(org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStopWorkerMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class BeamFnExternalWorkerPoolFutureStub extends io.grpc.stub.AbstractFutureStub<BeamFnExternalWorkerPoolFutureStub> {
    private BeamFnExternalWorkerPoolFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected BeamFnExternalWorkerPoolFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new BeamFnExternalWorkerPoolFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Start the SDK worker with the given ID.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse> startWorker(
        org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartWorkerMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Stop the SDK worker.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse> stopWorker(
        org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStopWorkerMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_START_WORKER = 0;
  private static final int METHODID_STOP_WORKER = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BeamFnExternalWorkerPoolImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(BeamFnExternalWorkerPoolImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_START_WORKER:
          serviceImpl.startWorker((org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StartWorkerResponse>) responseObserver);
          break;
        case METHODID_STOP_WORKER:
          serviceImpl.stopWorker((org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.fnexecution.v1.BeamFnApi.StopWorkerResponse>) responseObserver);
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

  private static abstract class BeamFnExternalWorkerPoolBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    BeamFnExternalWorkerPoolBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.fnexecution.v1.BeamFnApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("BeamFnExternalWorkerPool");
    }
  }

  private static final class BeamFnExternalWorkerPoolFileDescriptorSupplier
      extends BeamFnExternalWorkerPoolBaseDescriptorSupplier {
    BeamFnExternalWorkerPoolFileDescriptorSupplier() {}
  }

  private static final class BeamFnExternalWorkerPoolMethodDescriptorSupplier
      extends BeamFnExternalWorkerPoolBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    BeamFnExternalWorkerPoolMethodDescriptorSupplier(String methodName) {
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
      synchronized (BeamFnExternalWorkerPoolGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new BeamFnExternalWorkerPoolFileDescriptorSupplier())
              .addMethod(getStartWorkerMethod())
              .addMethod(getStopWorkerMethod())
              .build();
        }
      }
    }
    return result;
  }
}
