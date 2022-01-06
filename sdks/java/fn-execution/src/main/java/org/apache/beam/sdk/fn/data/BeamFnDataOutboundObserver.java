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
package org.apache.beam.sdk.fn.data;

import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString.Output;

/**
 * An outbound {@link FnDataReceiver} for the Beam Fn Data API.
 *
 * <p>TODO: Handle outputting large elements (&gt; 2GiBs). Note that this also applies to the input
 * side as well.
 *
 * <p>TODO: Handle outputting elements that are zero bytes by outputting a single byte as a marker,
 * detect on the input side that no bytes were read and force reading a single byte.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamFnDataOutboundObserver<T> implements CloseableFnDataReceiver<T> {

  private boolean closed;
  private final Coder<T> coder;
  private final LogicalEndpoint outputLocation;
  private final BeamFnDataOutboundAggregator aggregator;

  public BeamFnDataOutboundObserver(
      LogicalEndpoint outputLocation, Coder<T> coder, BeamFnDataOutboundAggregator aggregator) {
    this.outputLocation = outputLocation;
    this.coder = coder;
    this.aggregator = aggregator;
    this.aggregator.registerOutputLocation(outputLocation);
    this.closed = false;
  }

  @Override
  public void close() throws Exception {
    this.closed = true;
    this.aggregator.unregisterOutputLocation(outputLocation);
  }

  @Override
  public void flush() throws IOException {}

  @Override
  public void accept(T t) throws Exception {
    if (closed) {
      throw new IllegalStateException("Already closed.");
    }
    Output output = ByteString.newOutput();
    coder.encode(t, output);
    aggregator.accept(outputLocation, output);
  }
}
