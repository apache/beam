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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ClientInterceptor;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.MethodDescriptor;

/**
 * Intercepts outgoing calls and attaches a relative deadline to the call. Deadlines are absolute,
 * so they must be created and attached with every RPC call. The alternative to registering an
 * {@link ClientInterceptor} would be to remember to call stub.withDeadlineAfter(...) at every call
 * site. For more context see <a
 * href=https://github.com/grpc/grpc-java/issues/1495>grpc-java/issues/1495</a> and <a
 * href=https://github.com/grpc/grpc-java/issues/4305>grpc-java/issues/4305</a>.
 *
 * @see <a href=https://grpc.io/blog/deadlines/>Official gRPC Deadline Documentation.</a>
 */
class GrpcDeadlineClientInterceptor implements ClientInterceptor {
  private static final int DEFAULT_UNARY_RPC_DEADLINE_SECONDS = 10;
  private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

  private final int duration;
  private final TimeUnit timeUnit;

  GrpcDeadlineClientInterceptor(int duration, TimeUnit timeUnit) {
    this.duration = duration;
    this.timeUnit = timeUnit;
  }

  static GrpcDeadlineClientInterceptor withDefaultUnaryRpcDeadline() {
    return new GrpcDeadlineClientInterceptor(DEFAULT_UNARY_RPC_DEADLINE_SECONDS, DEFAULT_TIME_UNIT);
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel next) {
    return next.newCall(methodDescriptor, callOptions.withDeadlineAfter(duration, timeUnit));
  }
}
