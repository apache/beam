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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import java.io.IOException;
import java.io.InputStream;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.MethodDescriptor.Marshaller;

/**
 * {@link NoopClientCall} is a class that is designed for use in tests. It is designed to be used in
 * places where a scriptable call is necessary. By default, all methods are noops, and designed to
 * be overridden.
 */
public class NoopClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

  /**
   * {@link NoopClientCall.NoopClientCallListener} is a class that is designed for use in tests. It
   * is designed to be used in places where a scriptable call listener is necessary. By default, all
   * methods are noops, and designed to be overridden.
   */
  public static class NoopClientCallListener<T> extends ClientCall.Listener<T> {}

  public static class NoopMarshaller implements Marshaller<Object> {

    @Override
    public InputStream stream(Object o) {
      return new InputStream() {
        @Override
        public int read() throws IOException {
          return 0;
        }
      };
    }

    @Override
    public Object parse(InputStream inputStream) {
      return null;
    }
  }

  @Override
  public void start(ClientCall.Listener<RespT> listener, Metadata headers) {}

  @Override
  public void request(int numMessages) {}

  @Override
  public void cancel(String message, Throwable cause) {}

  @Override
  public void halfClose() {}

  @Override
  public void sendMessage(ReqT message) {}
}
