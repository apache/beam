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
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.stub.StreamObserver;

/**
 * An outbound {@link FnDataReceiver} for the Beam Fn Data API.
 *
 * <p>TODO: Handle outputting large elements (&gt; 2GiBs). Note that this also applies to the input
 * side as well.
 *
 * <p>TODO: Handle outputting elements that are zero bytes by outputting a single byte as a marker,
 * detect on the input side that no bytes were read and force reading a single byte.
 *
 * @deprecated Migrate to use {@link BeamFnDataOutboundAggregator} directly.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Deprecated
public class BeamFnDataOutboundObserver<T> implements CloseableFnDataReceiver<T> {

  private boolean closed;
  private final BeamFnDataOutboundAggregator aggregator;
  private final FnDataReceiver<T> dataReceiver;

  public BeamFnDataOutboundObserver(
      LogicalEndpoint outputLocation,
      Coder<T> coder,
      StreamObserver<Elements> outboundObserver,
      PipelineOptions options) {
    this.aggregator =
        new BeamFnDataOutboundAggregator(
            options, outputLocation::getInstructionId, outboundObserver, false);
    this.dataReceiver =
        outputLocation.isTimer()
            ? (FnDataReceiver<T>)
                this.aggregator.registerOutputTimersLocation(
                    outputLocation.getTransformId(),
                    outputLocation.getTimerFamilyId(),
                    (Coder<Object>) coder)
            : (FnDataReceiver<T>)
                aggregator.registerOutputDataLocation(
                    outputLocation.getTransformId(), (Coder<Object>) coder);
    aggregator.start();
    this.closed = false;
  }

  @Override
  public void close() throws Exception {
    this.aggregator.sendOrCollectBufferedDataAndFinishOutboundStreams();
    this.closed = true;
  }

  @Override
  public void flush() throws IOException {
    aggregator.flush();
  }

  @Override
  public void accept(T t) throws Exception {
    if (closed) {
      throw new IllegalStateException("Already closed.");
    }
    dataReceiver.accept(t);
  }
}
