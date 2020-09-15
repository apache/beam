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
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A size-based buffering outbound {@link FnDataReceiver} for the Beam Fn Data API.
 *
 * <p>TODO: Handle outputting large elements (&gt; 2GiBs). Note that this also applies to the input
 * side as well.
 *
 * <p>TODO: Handle outputting elements that are zero bytes by outputting a single byte as a marker,
 * detect on the input side that no bytes were read and force reading a single byte.
 */
public class BeamFnDataSizeBasedBufferingOutboundObserver<T>
    implements BeamFnDataBufferingOutboundObserver<T> {
  private static final Logger LOG =
      LoggerFactory.getLogger(BeamFnDataSizeBasedBufferingOutboundObserver.class);

  private long byteCounter;
  private long counter;
  private boolean closed;
  private final int sizeLimit;
  private final Coder<T> coder;
  private final LogicalEndpoint outputLocation;
  private final StreamObserver<BeamFnApi.Elements> outboundObserver;
  private final ByteString.Output bufferedElements;

  BeamFnDataSizeBasedBufferingOutboundObserver(
      int sizeLimit,
      LogicalEndpoint outputLocation,
      Coder<T> coder,
      StreamObserver<BeamFnApi.Elements> outboundObserver) {
    this.sizeLimit = sizeLimit;
    this.outputLocation = outputLocation;
    this.coder = coder;
    this.outboundObserver = outboundObserver;
    this.bufferedElements = ByteString.newOutput();
    this.closed = false;
  }

  @Override
  public void close() throws Exception {
    if (closed) {
      return;
    }
    closed = true;
    BeamFnApi.Elements.Builder elements = convertBufferForTransmission();
    // This will add an empty data block representing the end of stream.
    if (outputLocation.isTimer()) {
      elements
          .addTimersBuilder()
          .setInstructionId(outputLocation.getInstructionId())
          .setTransformId(outputLocation.getTransformId())
          .setTimerFamilyId(outputLocation.getTimerFamilyId())
          .setIsLast(true);
    } else {
      elements
          .addDataBuilder()
          .setInstructionId(outputLocation.getInstructionId())
          .setTransformId(outputLocation.getTransformId())
          .setIsLast(true);
    }

    LOG.debug(
        "Closing stream for instruction {} and "
            + "transform {} having transmitted {} values {} bytes",
        outputLocation.getInstructionId(),
        outputLocation.getTransformId(),
        counter,
        byteCounter);
    outboundObserver.onNext(elements.build());
  }

  @Override
  public void flush() throws IOException {
    if (bufferedElements.size() > 0) {
      outboundObserver.onNext(convertBufferForTransmission().build());
    }
  }

  @Override
  public void accept(T t) throws IOException {
    if (closed) {
      throw new IllegalStateException("Already closed.");
    }
    coder.encode(t, bufferedElements);
    counter += 1;
    if (bufferedElements.size() >= sizeLimit) {
      flush();
    }
  }

  private BeamFnApi.Elements.Builder convertBufferForTransmission() {
    BeamFnApi.Elements.Builder elements = BeamFnApi.Elements.newBuilder();
    if (bufferedElements.size() == 0) {
      return elements;
    }

    if (outputLocation.isTimer()) {
      elements
          .addTimersBuilder()
          .setInstructionId(outputLocation.getInstructionId())
          .setTransformId(outputLocation.getTransformId())
          .setTimerFamilyId(outputLocation.getTimerFamilyId())
          .setTimers(bufferedElements.toByteString());
    } else {
      elements
          .addDataBuilder()
          .setInstructionId(outputLocation.getInstructionId())
          .setTransformId(outputLocation.getTransformId())
          .setData(bufferedElements.toByteString());
    }

    byteCounter += bufferedElements.size();
    bufferedElements.reset();
    return elements;
  }
}
