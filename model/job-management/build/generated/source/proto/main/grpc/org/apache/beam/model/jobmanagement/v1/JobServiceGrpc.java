package org.apache.beam.model.jobmanagement.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Job Service for running RunnerAPI pipelines
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.48.1)",
    comments = "Source: org/apache/beam/model/job_management/v1/beam_job_api.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class JobServiceGrpc {

  private JobServiceGrpc() {}

  public static final String SERVICE_NAME = "org.apache.beam.model.job_management.v1.JobService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse> getPrepareMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Prepare",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse> getPrepareMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest, org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse> getPrepareMethod;
    if ((getPrepareMethod = JobServiceGrpc.getPrepareMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getPrepareMethod = JobServiceGrpc.getPrepareMethod) == null) {
          JobServiceGrpc.getPrepareMethod = getPrepareMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest, org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Prepare"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("Prepare"))
              .build();
        }
      }
    }
    return getPrepareMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse> getRunMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Run",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse> getRunMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest, org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse> getRunMethod;
    if ((getRunMethod = JobServiceGrpc.getRunMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getRunMethod = JobServiceGrpc.getRunMethod) == null) {
          JobServiceGrpc.getRunMethod = getRunMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest, org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Run"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("Run"))
              .build();
        }
      }
    }
    return getRunMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse> getGetJobsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetJobs",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse> getGetJobsMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest, org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse> getGetJobsMethod;
    if ((getGetJobsMethod = JobServiceGrpc.getGetJobsMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getGetJobsMethod = JobServiceGrpc.getGetJobsMethod) == null) {
          JobServiceGrpc.getGetJobsMethod = getGetJobsMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest, org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetJobs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("GetJobs"))
              .build();
        }
      }
    }
    return getGetJobsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> getGetStateMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetState",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> getGetStateMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest, org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> getGetStateMethod;
    if ((getGetStateMethod = JobServiceGrpc.getGetStateMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getGetStateMethod = JobServiceGrpc.getGetStateMethod) == null) {
          JobServiceGrpc.getGetStateMethod = getGetStateMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest, org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetState"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("GetState"))
              .build();
        }
      }
    }
    return getGetStateMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse> getGetPipelineMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPipeline",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse> getGetPipelineMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest, org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse> getGetPipelineMethod;
    if ((getGetPipelineMethod = JobServiceGrpc.getGetPipelineMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getGetPipelineMethod = JobServiceGrpc.getGetPipelineMethod) == null) {
          JobServiceGrpc.getGetPipelineMethod = getGetPipelineMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest, org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPipeline"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("GetPipeline"))
              .build();
        }
      }
    }
    return getGetPipelineMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse> getCancelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Cancel",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse> getCancelMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest, org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse> getCancelMethod;
    if ((getCancelMethod = JobServiceGrpc.getCancelMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getCancelMethod = JobServiceGrpc.getCancelMethod) == null) {
          JobServiceGrpc.getCancelMethod = getCancelMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest, org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Cancel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("Cancel"))
              .build();
        }
      }
    }
    return getCancelMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> getGetStateStreamMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetStateStream",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> getGetStateStreamMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest, org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> getGetStateStreamMethod;
    if ((getGetStateStreamMethod = JobServiceGrpc.getGetStateStreamMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getGetStateStreamMethod = JobServiceGrpc.getGetStateStreamMethod) == null) {
          JobServiceGrpc.getGetStateStreamMethod = getGetStateStreamMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest, org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetStateStream"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("GetStateStream"))
              .build();
        }
      }
    }
    return getGetStateStreamMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse> getGetMessageStreamMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMessageStream",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse> getGetMessageStreamMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest, org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse> getGetMessageStreamMethod;
    if ((getGetMessageStreamMethod = JobServiceGrpc.getGetMessageStreamMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getGetMessageStreamMethod = JobServiceGrpc.getGetMessageStreamMethod) == null) {
          JobServiceGrpc.getGetMessageStreamMethod = getGetMessageStreamMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest, org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMessageStream"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("GetMessageStream"))
              .build();
        }
      }
    }
    return getGetMessageStreamMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse> getGetJobMetricsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetJobMetrics",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse> getGetJobMetricsMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest, org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse> getGetJobMetricsMethod;
    if ((getGetJobMetricsMethod = JobServiceGrpc.getGetJobMetricsMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getGetJobMetricsMethod = JobServiceGrpc.getGetJobMetricsMethod) == null) {
          JobServiceGrpc.getGetJobMetricsMethod = getGetJobMetricsMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest, org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetJobMetrics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("GetJobMetrics"))
              .build();
        }
      }
    }
    return getGetJobMetricsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse> getDescribePipelineOptionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DescribePipelineOptions",
      requestType = org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest.class,
      responseType = org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest,
      org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse> getDescribePipelineOptionsMethod() {
    io.grpc.MethodDescriptor<org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest, org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse> getDescribePipelineOptionsMethod;
    if ((getDescribePipelineOptionsMethod = JobServiceGrpc.getDescribePipelineOptionsMethod) == null) {
      synchronized (JobServiceGrpc.class) {
        if ((getDescribePipelineOptionsMethod = JobServiceGrpc.getDescribePipelineOptionsMethod) == null) {
          JobServiceGrpc.getDescribePipelineOptionsMethod = getDescribePipelineOptionsMethod =
              io.grpc.MethodDescriptor.<org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest, org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DescribePipelineOptions"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JobServiceMethodDescriptorSupplier("DescribePipelineOptions"))
              .build();
        }
      }
    }
    return getDescribePipelineOptionsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static JobServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JobServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JobServiceStub>() {
        @java.lang.Override
        public JobServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JobServiceStub(channel, callOptions);
        }
      };
    return JobServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static JobServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JobServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JobServiceBlockingStub>() {
        @java.lang.Override
        public JobServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JobServiceBlockingStub(channel, callOptions);
        }
      };
    return JobServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static JobServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JobServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JobServiceFutureStub>() {
        @java.lang.Override
        public JobServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JobServiceFutureStub(channel, callOptions);
        }
      };
    return JobServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Job Service for running RunnerAPI pipelines
   * </pre>
   */
  public static abstract class JobServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Prepare a job for execution. The job will not be executed until a call is made to run with the
     * returned preparationId.
     * </pre>
     */
    public void prepare(org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPrepareMethod(), responseObserver);
    }

    /**
     * <pre>
     * Submit the job for execution
     * </pre>
     */
    public void run(org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRunMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get a list of all invoked jobs
     * </pre>
     */
    public void getJobs(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetJobsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get the current state of the job
     * </pre>
     */
    public void getState(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetStateMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get the job's pipeline
     * </pre>
     */
    public void getPipeline(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPipelineMethod(), responseObserver);
    }

    /**
     * <pre>
     * Cancel the job
     * </pre>
     */
    public void cancel(org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCancelMethod(), responseObserver);
    }

    /**
     * <pre>
     * Subscribe to a stream of state changes of the job, will immediately return the current state of the job as the first response.
     * </pre>
     */
    public void getStateStream(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetStateStreamMethod(), responseObserver);
    }

    /**
     * <pre>
     * Subscribe to a stream of state changes and messages from the job
     * </pre>
     */
    public void getMessageStream(org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMessageStreamMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch metrics for a given job
     * </pre>
     */
    public void getJobMetrics(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetJobMetricsMethod(), responseObserver);
    }

    /**
     * <pre>
     * Get the supported pipeline options of the runner
     * </pre>
     */
    public void describePipelineOptions(org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDescribePipelineOptionsMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getPrepareMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse>(
                  this, METHODID_PREPARE)))
          .addMethod(
            getRunMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse>(
                  this, METHODID_RUN)))
          .addMethod(
            getGetJobsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse>(
                  this, METHODID_GET_JOBS)))
          .addMethod(
            getGetStateMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent>(
                  this, METHODID_GET_STATE)))
          .addMethod(
            getGetPipelineMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse>(
                  this, METHODID_GET_PIPELINE)))
          .addMethod(
            getCancelMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse>(
                  this, METHODID_CANCEL)))
          .addMethod(
            getGetStateStreamMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent>(
                  this, METHODID_GET_STATE_STREAM)))
          .addMethod(
            getGetMessageStreamMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse>(
                  this, METHODID_GET_MESSAGE_STREAM)))
          .addMethod(
            getGetJobMetricsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse>(
                  this, METHODID_GET_JOB_METRICS)))
          .addMethod(
            getDescribePipelineOptionsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest,
                org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse>(
                  this, METHODID_DESCRIBE_PIPELINE_OPTIONS)))
          .build();
    }
  }

  /**
   * <pre>
   * Job Service for running RunnerAPI pipelines
   * </pre>
   */
  public static final class JobServiceStub extends io.grpc.stub.AbstractAsyncStub<JobServiceStub> {
    private JobServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JobServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Prepare a job for execution. The job will not be executed until a call is made to run with the
     * returned preparationId.
     * </pre>
     */
    public void prepare(org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPrepareMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Submit the job for execution
     * </pre>
     */
    public void run(org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRunMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get a list of all invoked jobs
     * </pre>
     */
    public void getJobs(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetJobsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get the current state of the job
     * </pre>
     */
    public void getState(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetStateMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get the job's pipeline
     * </pre>
     */
    public void getPipeline(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPipelineMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Cancel the job
     * </pre>
     */
    public void cancel(org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Subscribe to a stream of state changes of the job, will immediately return the current state of the job as the first response.
     * </pre>
     */
    public void getStateStream(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getGetStateStreamMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Subscribe to a stream of state changes and messages from the job
     * </pre>
     */
    public void getMessageStream(org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getGetMessageStreamMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch metrics for a given job
     * </pre>
     */
    public void getJobMetrics(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetJobMetricsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Get the supported pipeline options of the runner
     * </pre>
     */
    public void describePipelineOptions(org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest request,
        io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDescribePipelineOptionsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Job Service for running RunnerAPI pipelines
   * </pre>
   */
  public static final class JobServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<JobServiceBlockingStub> {
    private JobServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JobServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Prepare a job for execution. The job will not be executed until a call is made to run with the
     * returned preparationId.
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse prepare(org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrepareMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Submit the job for execution
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse run(org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRunMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get a list of all invoked jobs
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse getJobs(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetJobsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get the current state of the job
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent getState(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetStateMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get the job's pipeline
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse getPipeline(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPipelineMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Cancel the job
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse cancel(org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCancelMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Subscribe to a stream of state changes of the job, will immediately return the current state of the job as the first response.
     * </pre>
     */
    public java.util.Iterator<org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> getStateStream(
        org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getGetStateStreamMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Subscribe to a stream of state changes and messages from the job
     * </pre>
     */
    public java.util.Iterator<org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse> getMessageStream(
        org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getGetMessageStreamMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch metrics for a given job
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse getJobMetrics(org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetJobMetricsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Get the supported pipeline options of the runner
     * </pre>
     */
    public org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse describePipelineOptions(org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDescribePipelineOptionsMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Job Service for running RunnerAPI pipelines
   * </pre>
   */
  public static final class JobServiceFutureStub extends io.grpc.stub.AbstractFutureStub<JobServiceFutureStub> {
    private JobServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JobServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JobServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Prepare a job for execution. The job will not be executed until a call is made to run with the
     * returned preparationId.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse> prepare(
        org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPrepareMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Submit the job for execution
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse> run(
        org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRunMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get a list of all invoked jobs
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse> getJobs(
        org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetJobsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get the current state of the job
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent> getState(
        org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetStateMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get the job's pipeline
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse> getPipeline(
        org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPipelineMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Cancel the job
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse> cancel(
        org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCancelMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch metrics for a given job
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse> getJobMetrics(
        org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetJobMetricsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Get the supported pipeline options of the runner
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse> describePipelineOptions(
        org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDescribePipelineOptionsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PREPARE = 0;
  private static final int METHODID_RUN = 1;
  private static final int METHODID_GET_JOBS = 2;
  private static final int METHODID_GET_STATE = 3;
  private static final int METHODID_GET_PIPELINE = 4;
  private static final int METHODID_CANCEL = 5;
  private static final int METHODID_GET_STATE_STREAM = 6;
  private static final int METHODID_GET_MESSAGE_STREAM = 7;
  private static final int METHODID_GET_JOB_METRICS = 8;
  private static final int METHODID_DESCRIBE_PIPELINE_OPTIONS = 9;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final JobServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(JobServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PREPARE:
          serviceImpl.prepare((org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse>) responseObserver);
          break;
        case METHODID_RUN:
          serviceImpl.run((org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse>) responseObserver);
          break;
        case METHODID_GET_JOBS:
          serviceImpl.getJobs((org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobsResponse>) responseObserver);
          break;
        case METHODID_GET_STATE:
          serviceImpl.getState((org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent>) responseObserver);
          break;
        case METHODID_GET_PIPELINE:
          serviceImpl.getPipeline((org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobPipelineResponse>) responseObserver);
          break;
        case METHODID_CANCEL:
          serviceImpl.cancel((org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.CancelJobResponse>) responseObserver);
          break;
        case METHODID_GET_STATE_STREAM:
          serviceImpl.getStateStream((org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.JobStateEvent>) responseObserver);
          break;
        case METHODID_GET_MESSAGE_STREAM:
          serviceImpl.getMessageStream((org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.JobMessagesResponse>) responseObserver);
          break;
        case METHODID_GET_JOB_METRICS:
          serviceImpl.getJobMetrics((org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.GetJobMetricsResponse>) responseObserver);
          break;
        case METHODID_DESCRIBE_PIPELINE_OPTIONS:
          serviceImpl.describePipelineOptions((org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.beam.model.jobmanagement.v1.JobApi.DescribePipelineOptionsResponse>) responseObserver);
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

  private static abstract class JobServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    JobServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.beam.model.jobmanagement.v1.JobApi.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("JobService");
    }
  }

  private static final class JobServiceFileDescriptorSupplier
      extends JobServiceBaseDescriptorSupplier {
    JobServiceFileDescriptorSupplier() {}
  }

  private static final class JobServiceMethodDescriptorSupplier
      extends JobServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    JobServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (JobServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new JobServiceFileDescriptorSupplier())
              .addMethod(getPrepareMethod())
              .addMethod(getRunMethod())
              .addMethod(getGetJobsMethod())
              .addMethod(getGetStateMethod())
              .addMethod(getGetPipelineMethod())
              .addMethod(getCancelMethod())
              .addMethod(getGetStateStreamMethod())
              .addMethod(getGetMessageStreamMethod())
              .addMethod(getGetJobMetricsMethod())
              .addMethod(getDescribePipelineOptionsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
