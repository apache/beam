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

import java.io.InputStream;
import java.util.function.BiConsumer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes individually consumed {@link ByteString}s with the provided {@link Coder} passing the
 * individual decoded elements to the provided consumer.
 */
public class BeamFnDataInboundObserver<T>
    implements BiConsumer<ByteString, Boolean>, InboundDataClient {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataInboundObserver.class);

  public static <T> BeamFnDataInboundObserver<T> forConsumer(
      LogicalEndpoint endpoint, Coder<T> coder, FnDataReceiver<T> receiver) {
    return new BeamFnDataInboundObserver<>(
        endpoint, coder, receiver, CompletableFutureInboundDataClient.create());
  }

  private final LogicalEndpoint endpoint;
  private final FnDataReceiver<T> consumer;
  private final Coder<T> coder;
  private final InboundDataClient readFuture;
  private long byteCounter;
  private long counter;

  public BeamFnDataInboundObserver(
      LogicalEndpoint endpoint,
      Coder<T> coder,
      FnDataReceiver<T> consumer,
      InboundDataClient readFuture) {
    this.endpoint = endpoint;
    this.coder = coder;
    this.consumer = consumer;
    this.readFuture = readFuture;
  }

  @Override
  public void accept(ByteString payload, Boolean isLast) {
    if (readFuture.isDone()) {
      // Drop any incoming data if the stream processing has finished.
      return;
    }
    try {
      if (isLast) {
        LOG.debug(
            "Closing stream for {} having consumed {} values {} bytes",
            endpoint,
            counter,
            byteCounter);
        readFuture.complete();
        return;
      }

      byteCounter += payload.size();
      InputStream inputStream = payload.newInput();
      while (inputStream.available() > 0) {
        counter += 1;
        T value = coder.decode(inputStream);
        consumer.accept(value);
      }
    } catch (Exception e) {
      readFuture.fail(e);
    }
  }

  @Override
  public void awaitCompletion() throws Exception {
    readFuture.awaitCompletion();
  }

  @Override
  public boolean isDone() {
    return readFuture.isDone();
  }

  @Override
  public void cancel() {
    readFuture.cancel();
  }

  @Override
  public void complete() {
    readFuture.complete();
  }

  @Override
  public void fail(Throwable t) {
    readFuture.fail(t);
  }
}
