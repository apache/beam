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
package org.apache.beam.runners.fnexecution;

import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Context;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Contexts;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Metadata.Key;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ServerCall;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ServerCall.Listener;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ServerCallHandler;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ServerInterceptor;

/**
 * A HeaderAccessorProvider which intercept the header in a GRPC request and expose the relevant
 * fields.
 */
public class GrpcContextHeaderAccessorProvider {

  private static final Key<String> WORKER_ID_KEY =
      Key.of("worker_id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Context.Key<String> SDK_WORKER_CONTEXT_KEY = Context.key("worker_id");
  private static final GrpcHeaderAccessor HEADER_ACCESSOR = new GrpcHeaderAccessor();
  private static final ServerInterceptor INTERCEPTOR =
      new ServerInterceptor() {
        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata requestHeaders,
            ServerCallHandler<ReqT, RespT> next) {
          String workerId = requestHeaders.get(WORKER_ID_KEY);
          Context context = Context.current().withValue(SDK_WORKER_CONTEXT_KEY, workerId);
          return Contexts.interceptCall(context, call, requestHeaders, next);
        }
      };

  public static ServerInterceptor interceptor() {
    return INTERCEPTOR;
  }

  public static HeaderAccessor getHeaderAccessor() {
    return HEADER_ACCESSOR;
  }

  private static class GrpcHeaderAccessor implements HeaderAccessor {

    @Override
    /** This method should be called from the request method. */
    public String getSdkWorkerId() {
      // TODO: https://issues.apache.org/jira/browse/BEAM-4149 Some harnesses may not set the worker
      // id header. Remove the null check below once this is fixed.
      String workerId = SDK_WORKER_CONTEXT_KEY.get();
      if (workerId == null) {
        return "";
      } else {
        return workerId;
      }
    }
  }
}
