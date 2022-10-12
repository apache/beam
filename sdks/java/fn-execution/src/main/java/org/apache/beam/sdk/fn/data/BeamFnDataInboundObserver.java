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

import java.util.function.BiConsumer;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes individually consumed {@link ByteString}s with the provided {@link Coder} passing the
 * individual decoded elements to the provided consumer.
 *
 * @deprecated Migrate to {@link BeamFnDataInboundObserver2}.
 */
@Deprecated
public class BeamFnDataInboundObserver
    implements BiConsumer<ByteString, Boolean>, InboundDataClient {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataInboundObserver.class);

  public static BeamFnDataInboundObserver forConsumer(
      LogicalEndpoint endpoint, FnDataReceiver<ByteString> receiver) {
    return new BeamFnDataInboundObserver(
        endpoint, receiver, CompletableFutureInboundDataClient.create());
  }

  private final LogicalEndpoint endpoint;
  private final FnDataReceiver<ByteString> consumer;
  private final InboundDataClient readFuture;
  private long byteCounter;

  public BeamFnDataInboundObserver(
      LogicalEndpoint endpoint, FnDataReceiver<ByteString> consumer, InboundDataClient readFuture) {
    this.endpoint = endpoint;
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
        LOG.debug("Closing stream for {} having consumed {} bytes", endpoint, byteCounter);
        readFuture.complete();
        return;
      }

      byteCounter += payload.size();
      consumer.accept(payload);
    } catch (Exception e) {
      readFuture.fail(e);
    }
  }

  @Override
  public void awaitCompletion() throws Exception {
    readFuture.awaitCompletion();
  }

  @Override
  public void runWhenComplete(Runnable completeRunnable) {
    readFuture.runWhenComplete(completeRunnable);
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
