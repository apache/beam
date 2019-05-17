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

import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Context;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Contexts;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ServerCall;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ServerCallHandler;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ServerInterceptor;
import org.apache.beam.vendor.grpc.v1p13p1.io.netty.util.internal.StringUtil;

/**
 * Token based authentication provider. This can be used for the gRPC channels between portable
 * runner and sdk workers. The two processes can share the token through file systems, where the
 * token can be protected even in a multi-tenant environment.
 */
public class GrpcFileTokenAuthProvider {
  private static final Metadata.Key<String> FS_TOKEN_KEY =
      Metadata.Key.of("fs_token", Metadata.ASCII_STRING_MARSHALLER);

  private GrpcFileTokenAuthProvider() {}

  public static ServerInterceptor interceptor(String token) {
    return new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call,
          Metadata requestHeaders,
          ServerCallHandler<ReqT, RespT> next) {
        String requestToken = requestHeaders.get(FS_TOKEN_KEY);
        if (StringUtil.isNullOrEmpty(requestToken)) {
          throw new RuntimeException("Null or empty request token.");
        }
        if (!token.equals(requestToken)) {
          throw new RuntimeException("Incorrect request token.");
        }
        return Contexts.interceptCall(Context.current(), call, requestHeaders, next);
      }
    };
  }
}
