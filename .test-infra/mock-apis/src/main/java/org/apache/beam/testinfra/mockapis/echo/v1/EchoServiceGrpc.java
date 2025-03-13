/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.testinfra.mockapis.echo.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 *
 *
 * <pre>
 * EchoService simulates a mock API that echos a request.
 * </pre>
 */
@SuppressWarnings({
  "argument",
  "assignment",
  "initialization.fields.uninitialized",
  "initialization.static.field.uninitialized",
  "override.param",
  "ClassTypeParameterName",
  "ForbidNonVendoredGuava",
  "JavadocStyle",
  "LocalVariableName",
  "MemberName",
  "NeedBraces",
  "MissingOverride",
  "RedundantModifier",
  "ReferenceEquality",
  "UnusedVariable",
})
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.58.0)",
    comments = "Source: proto/echo/v1/echo.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class EchoServiceGrpc {

  private EchoServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "proto.echo.v1.EchoService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest,
          org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse>
      getEchoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Echo",
      requestType = org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest.class,
      responseType = org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest,
          org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse>
      getEchoMethod() {
    io.grpc.MethodDescriptor<
            org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest,
            org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse>
        getEchoMethod;
    if ((getEchoMethod = EchoServiceGrpc.getEchoMethod) == null) {
      synchronized (EchoServiceGrpc.class) {
        if ((getEchoMethod = EchoServiceGrpc.getEchoMethod) == null) {
          EchoServiceGrpc.getEchoMethod =
              getEchoMethod =
                  io.grpc.MethodDescriptor
                      .<org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest,
                          org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse>
                          newBuilder()
                      .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                      .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Echo"))
                      .setSampledToLocalTracing(true)
                      .setRequestMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest
                                  .getDefaultInstance()))
                      .setResponseMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse
                                  .getDefaultInstance()))
                      .setSchemaDescriptor(new EchoServiceMethodDescriptorSupplier("Echo"))
                      .build();
        }
      }
    }
    return getEchoMethod;
  }

  /** Creates a new async stub that supports all call types for the service */
  public static EchoServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EchoServiceStub> factory =
        new io.grpc.stub.AbstractStub.StubFactory<EchoServiceStub>() {
          @java.lang.Override
          public EchoServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new EchoServiceStub(channel, callOptions);
          }
        };
    return EchoServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EchoServiceBlockingStub newBlockingStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EchoServiceBlockingStub> factory =
        new io.grpc.stub.AbstractStub.StubFactory<EchoServiceBlockingStub>() {
          @java.lang.Override
          public EchoServiceBlockingStub newStub(
              io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new EchoServiceBlockingStub(channel, callOptions);
          }
        };
    return EchoServiceBlockingStub.newStub(factory, channel);
  }

  /** Creates a new ListenableFuture-style stub that supports unary calls on the service */
  public static EchoServiceFutureStub newFutureStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EchoServiceFutureStub> factory =
        new io.grpc.stub.AbstractStub.StubFactory<EchoServiceFutureStub>() {
          @java.lang.Override
          public EchoServiceFutureStub newStub(
              io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new EchoServiceFutureStub(channel, callOptions);
          }
        };
    return EchoServiceFutureStub.newStub(factory, channel);
  }

  /**
   *
   *
   * <pre>
   * EchoService simulates a mock API that echos a request.
   * </pre>
   */
  public interface AsyncService {

    /**
     *
     *
     * <pre>
     * Echo an EchoRequest payload in an EchoResponse.
     * </pre>
     */
    default void echo(
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse>
            responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEchoMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service EchoService.
   *
   * <pre>
   * EchoService simulates a mock API that echos a request.
   * </pre>
   */
  public abstract static class EchoServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override
    public final io.grpc.ServerServiceDefinition bindService() {
      return EchoServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service EchoService.
   *
   * <pre>
   * EchoService simulates a mock API that echos a request.
   * </pre>
   */
  public static final class EchoServiceStub
      extends io.grpc.stub.AbstractAsyncStub<EchoServiceStub> {
    private EchoServiceStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EchoServiceStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EchoServiceStub(channel, callOptions);
    }

    /**
     *
     *
     * <pre>
     * Echo an EchoRequest payload in an EchoResponse.
     * </pre>
     */
    public void echo(
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse>
            responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEchoMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service EchoService.
   *
   * <pre>
   * EchoService simulates a mock API that echos a request.
   * </pre>
   */
  public static final class EchoServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<EchoServiceBlockingStub> {
    private EchoServiceBlockingStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EchoServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EchoServiceBlockingStub(channel, callOptions);
    }

    /**
     *
     *
     * <pre>
     * Echo an EchoRequest payload in an EchoResponse.
     * </pre>
     */
    public org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse echo(
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEchoMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service EchoService.
   *
   * <pre>
   * EchoService simulates a mock API that echos a request.
   * </pre>
   */
  public static final class EchoServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<EchoServiceFutureStub> {
    private EchoServiceFutureStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EchoServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EchoServiceFutureStub(channel, callOptions);
    }

    /**
     *
     *
     * <pre>
     * Echo an EchoRequest payload in an EchoResponse.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<
            org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse>
        echo(org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEchoMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ECHO = 0;

  private static final class MethodHandlers<Req, Resp>
      implements io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
          io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
          io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
          io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ECHO:
          serviceImpl.echo(
              (org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest) request,
              (io.grpc.stub.StreamObserver<
                      org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse>)
                  responseObserver);
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

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
            getEchoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
                new MethodHandlers<
                    org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest,
                    org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse>(
                    service, METHODID_ECHO)))
        .build();
  }

  private abstract static class EchoServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier,
          io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EchoServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.testinfra.mockapis.echo.v1.Echo.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EchoService");
    }
  }

  private static final class EchoServiceFileDescriptorSupplier
      extends EchoServiceBaseDescriptorSupplier {
    EchoServiceFileDescriptorSupplier() {}
  }

  private static final class EchoServiceMethodDescriptorSupplier
      extends EchoServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    EchoServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (EchoServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor =
              result =
                  io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
                      .setSchemaDescriptor(new EchoServiceFileDescriptorSupplier())
                      .addMethod(getEchoMethod())
                      .build();
        }
      }
    }
    return result;
  }
}
