package org.apache.beam.sdk.fn.channel;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;

/** Sets the worker_id header to the given value. */
public class WorkerIdInterceptor implements ClientInterceptor {
  private static final Key<String> WORKER_ID_KEY =
      Key.of("worker_id", Metadata.ASCII_STRING_MARSHALLER);

  private final String workerId;

  public WorkerIdInterceptor(String workerId) {
    this.workerId = workerId;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        headers.put(WORKER_ID_KEY, workerId);
        super.start(responseListener, headers);
      }
    };
  }
}
