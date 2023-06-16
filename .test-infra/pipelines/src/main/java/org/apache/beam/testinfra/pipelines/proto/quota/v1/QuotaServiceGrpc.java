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
package org.apache.beam.testinfra.pipelines.proto.quota.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 *
 *
 * <pre>
 * A QuotaService manages the internal cached quota state of the application.
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
    value = "by gRPC proto compiler (version 1.56.0)",
    comments = "Source: proto/quota/v1/quota.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class QuotaServiceGrpc {

  private QuotaServiceGrpc() {}

  public static final String SERVICE_NAME = "proto.quota.v1.QuotaService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse>
      getCreateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Create",
      requestType =
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest.class,
      responseType =
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse>
      getCreateMethod() {
    io.grpc.MethodDescriptor<
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest,
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse>
        getCreateMethod;
    if ((getCreateMethod = QuotaServiceGrpc.getCreateMethod) == null) {
      synchronized (QuotaServiceGrpc.class) {
        if ((getCreateMethod = QuotaServiceGrpc.getCreateMethod) == null) {
          QuotaServiceGrpc.getCreateMethod =
              getCreateMethod =
                  io.grpc.MethodDescriptor
                      .<org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                              .CreateRequest,
                          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                              .CreateResponse>
                          newBuilder()
                      .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                      .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Create"))
                      .setSampledToLocalTracing(true)
                      .setRequestMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                                  .CreateRequest.getDefaultInstance()))
                      .setResponseMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                                  .CreateResponse.getDefaultInstance()))
                      .setSchemaDescriptor(new QuotaServiceMethodDescriptorSupplier("Create"))
                      .build();
        }
      }
    }
    return getCreateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse>
      getListMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "List",
      requestType =
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest.class,
      responseType =
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse>
      getListMethod() {
    io.grpc.MethodDescriptor<
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest,
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse>
        getListMethod;
    if ((getListMethod = QuotaServiceGrpc.getListMethod) == null) {
      synchronized (QuotaServiceGrpc.class) {
        if ((getListMethod = QuotaServiceGrpc.getListMethod) == null) {
          QuotaServiceGrpc.getListMethod =
              getListMethod =
                  io.grpc.MethodDescriptor
                      .<org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                              .ListRequest,
                          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                              .ListResponse>
                          newBuilder()
                      .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                      .setFullMethodName(generateFullMethodName(SERVICE_NAME, "List"))
                      .setSampledToLocalTracing(true)
                      .setRequestMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                                  .ListRequest.getDefaultInstance()))
                      .setResponseMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                                  .ListResponse.getDefaultInstance()))
                      .setSchemaDescriptor(new QuotaServiceMethodDescriptorSupplier("List"))
                      .build();
        }
      }
    }
    return getListMethod;
  }

  private static volatile io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse>
      getDeleteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Delete",
      requestType =
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest.class,
      responseType =
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse>
      getDeleteMethod() {
    io.grpc.MethodDescriptor<
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest,
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse>
        getDeleteMethod;
    if ((getDeleteMethod = QuotaServiceGrpc.getDeleteMethod) == null) {
      synchronized (QuotaServiceGrpc.class) {
        if ((getDeleteMethod = QuotaServiceGrpc.getDeleteMethod) == null) {
          QuotaServiceGrpc.getDeleteMethod =
              getDeleteMethod =
                  io.grpc.MethodDescriptor
                      .<org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                              .DeleteRequest,
                          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                              .DeleteResponse>
                          newBuilder()
                      .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                      .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Delete"))
                      .setSampledToLocalTracing(true)
                      .setRequestMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                                  .DeleteRequest.getDefaultInstance()))
                      .setResponseMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                                  .DeleteResponse.getDefaultInstance()))
                      .setSchemaDescriptor(new QuotaServiceMethodDescriptorSupplier("Delete"))
                      .build();
        }
      }
    }
    return getDeleteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse>
      getDescribeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Describe",
      requestType =
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest.class,
      responseType =
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse>
      getDescribeMethod() {
    io.grpc.MethodDescriptor<
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest,
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse>
        getDescribeMethod;
    if ((getDescribeMethod = QuotaServiceGrpc.getDescribeMethod) == null) {
      synchronized (QuotaServiceGrpc.class) {
        if ((getDescribeMethod = QuotaServiceGrpc.getDescribeMethod) == null) {
          QuotaServiceGrpc.getDescribeMethod =
              getDescribeMethod =
                  io.grpc.MethodDescriptor
                      .<org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                              .DescribeRequest,
                          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                              .DescribeResponse>
                          newBuilder()
                      .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                      .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Describe"))
                      .setSampledToLocalTracing(true)
                      .setRequestMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                                  .DescribeRequest.getDefaultInstance()))
                      .setResponseMarshaller(
                          io.grpc.protobuf.ProtoUtils.marshaller(
                              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                                  .DescribeResponse.getDefaultInstance()))
                      .setSchemaDescriptor(new QuotaServiceMethodDescriptorSupplier("Describe"))
                      .build();
        }
      }
    }
    return getDescribeMethod;
  }

  /** Creates a new async stub that supports all call types for the service */
  public static QuotaServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QuotaServiceStub> factory =
        new io.grpc.stub.AbstractStub.StubFactory<QuotaServiceStub>() {
          @java.lang.Override
          public QuotaServiceStub newStub(
              io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new QuotaServiceStub(channel, callOptions);
          }
        };
    return QuotaServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static QuotaServiceBlockingStub newBlockingStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QuotaServiceBlockingStub> factory =
        new io.grpc.stub.AbstractStub.StubFactory<QuotaServiceBlockingStub>() {
          @java.lang.Override
          public QuotaServiceBlockingStub newStub(
              io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new QuotaServiceBlockingStub(channel, callOptions);
          }
        };
    return QuotaServiceBlockingStub.newStub(factory, channel);
  }

  /** Creates a new ListenableFuture-style stub that supports unary calls on the service */
  public static QuotaServiceFutureStub newFutureStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<QuotaServiceFutureStub> factory =
        new io.grpc.stub.AbstractStub.StubFactory<QuotaServiceFutureStub>() {
          @java.lang.Override
          public QuotaServiceFutureStub newStub(
              io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new QuotaServiceFutureStub(channel, callOptions);
          }
        };
    return QuotaServiceFutureStub.newStub(factory, channel);
  }

  /**
   *
   *
   * <pre>
   * A QuotaService manages the internal cached quota state of the application.
   * </pre>
   */
  public interface AsyncService {

    /**
     *
     *
     * <pre>
     * Create a new quota entry.
     * </pre>
     */
    default void create(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest request,
        io.grpc.stub.StreamObserver<
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse>
            responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateMethod(), responseObserver);
    }

    /**
     *
     *
     * <pre>
     * List available quota entries.
     * </pre>
     */
    default void list(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest request,
        io.grpc.stub.StreamObserver<
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse>
            responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListMethod(), responseObserver);
    }

    /**
     *
     *
     * <pre>
     * Delete a quota entry.
     * </pre>
     */
    default void delete(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest request,
        io.grpc.stub.StreamObserver<
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse>
            responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteMethod(), responseObserver);
    }

    /**
     *
     *
     * <pre>
     * Describe a quota entry.
     * </pre>
     */
    default void describe(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest request,
        io.grpc.stub.StreamObserver<
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse>
            responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDescribeMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service QuotaService.
   *
   * <pre>
   * A QuotaService manages the internal cached quota state of the application.
   * </pre>
   */
  public abstract static class QuotaServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override
    public final io.grpc.ServerServiceDefinition bindService() {
      return QuotaServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service QuotaService.
   *
   * <pre>
   * A QuotaService manages the internal cached quota state of the application.
   * </pre>
   */
  public static final class QuotaServiceStub
      extends io.grpc.stub.AbstractAsyncStub<QuotaServiceStub> {
    private QuotaServiceStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QuotaServiceStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QuotaServiceStub(channel, callOptions);
    }

    /**
     *
     *
     * <pre>
     * Create a new quota entry.
     * </pre>
     */
    public void create(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest request,
        io.grpc.stub.StreamObserver<
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse>
            responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     *
     *
     * <pre>
     * List available quota entries.
     * </pre>
     */
    public void list(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest request,
        io.grpc.stub.StreamObserver<
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse>
            responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     *
     *
     * <pre>
     * Delete a quota entry.
     * </pre>
     */
    public void delete(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest request,
        io.grpc.stub.StreamObserver<
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse>
            responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     *
     *
     * <pre>
     * Describe a quota entry.
     * </pre>
     */
    public void describe(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest request,
        io.grpc.stub.StreamObserver<
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse>
            responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDescribeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service QuotaService.
   *
   * <pre>
   * A QuotaService manages the internal cached quota state of the application.
   * </pre>
   */
  public static final class QuotaServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<QuotaServiceBlockingStub> {
    private QuotaServiceBlockingStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QuotaServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QuotaServiceBlockingStub(channel, callOptions);
    }

    /**
     *
     *
     * <pre>
     * Create a new quota entry.
     * </pre>
     */
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse create(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateMethod(), getCallOptions(), request);
    }

    /**
     *
     *
     * <pre>
     * List available quota entries.
     * </pre>
     */
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse list(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListMethod(), getCallOptions(), request);
    }

    /**
     *
     *
     * <pre>
     * Delete a quota entry.
     * </pre>
     */
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse delete(
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteMethod(), getCallOptions(), request);
    }

    /**
     *
     *
     * <pre>
     * Describe a quota entry.
     * </pre>
     */
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
        describe(
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
                request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDescribeMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service QuotaService.
   *
   * <pre>
   * A QuotaService manages the internal cached quota state of the application.
   * </pre>
   */
  public static final class QuotaServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<QuotaServiceFutureStub> {
    private QuotaServiceFutureStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected QuotaServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new QuotaServiceFutureStub(channel, callOptions);
    }

    /**
     *
     *
     * <pre>
     * Create a new quota entry.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse>
        create(
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
                request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateMethod(), getCallOptions()), request);
    }

    /**
     *
     *
     * <pre>
     * List available quota entries.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse>
        list(
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
                request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListMethod(), getCallOptions()), request);
    }

    /**
     *
     *
     * <pre>
     * Delete a quota entry.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse>
        delete(
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
                request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteMethod(), getCallOptions()), request);
    }

    /**
     *
     *
     * <pre>
     * Describe a quota entry.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse>
        describe(
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
                request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDescribeMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE = 0;
  private static final int METHODID_LIST = 1;
  private static final int METHODID_DELETE = 2;
  private static final int METHODID_DESCRIBE = 3;

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
        case METHODID_CREATE:
          serviceImpl.create(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest)
                  request,
              (io.grpc.stub.StreamObserver<
                      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                          .CreateResponse>)
                  responseObserver);
          break;
        case METHODID_LIST:
          serviceImpl.list(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest)
                  request,
              (io.grpc.stub.StreamObserver<
                      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                          .ListResponse>)
                  responseObserver);
          break;
        case METHODID_DELETE:
          serviceImpl.delete(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest)
                  request,
              (io.grpc.stub.StreamObserver<
                      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                          .DeleteResponse>)
                  responseObserver);
          break;
        case METHODID_DESCRIBE:
          serviceImpl.describe(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest)
                  request,
              (io.grpc.stub.StreamObserver<
                      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                          .DescribeResponse>)
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
            getCreateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
                new MethodHandlers<
                    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                        .CreateRequest,
                    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                        .CreateResponse>(service, METHODID_CREATE)))
        .addMethod(
            getListMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
                new MethodHandlers<
                    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest,
                    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                        .ListResponse>(service, METHODID_LIST)))
        .addMethod(
            getDeleteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
                new MethodHandlers<
                    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                        .DeleteRequest,
                    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                        .DeleteResponse>(service, METHODID_DELETE)))
        .addMethod(
            getDescribeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
                new MethodHandlers<
                    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                        .DescribeRequest,
                    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                        .DescribeResponse>(service, METHODID_DESCRIBE)))
        .build();
  }

  private abstract static class QuotaServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier,
          io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    QuotaServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("QuotaService");
    }
  }

  private static final class QuotaServiceFileDescriptorSupplier
      extends QuotaServiceBaseDescriptorSupplier {
    QuotaServiceFileDescriptorSupplier() {}
  }

  private static final class QuotaServiceMethodDescriptorSupplier
      extends QuotaServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    QuotaServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (QuotaServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor =
              result =
                  io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
                      .setSchemaDescriptor(new QuotaServiceFileDescriptorSupplier())
                      .addMethod(getCreateMethod())
                      .addMethod(getListMethod())
                      .addMethod(getDeleteMethod())
                      .addMethod(getDescribeMethod())
                      .build();
        }
      }
    }
    return result;
  }
}
