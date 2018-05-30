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
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes individually consumed {@link BeamFnApi.Elements.Data} with the provided {@link Coder}
 * passing the individual decoded elements to the provided consumer.
 */
public class BeamFnDataInboundObserver<T>
    implements Consumer<BeamFnApi.Elements.Data>, InboundDataClient {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataInboundObserver.class);

  public static <T> BeamFnDataInboundObserver<T> forConsumer(
      Coder<WindowedValue<T>> coder, FnDataReceiver<WindowedValue<T>> receiver) {
    return new BeamFnDataInboundObserver<>(
        coder, receiver, CompletableFutureInboundDataClient.create());
  }

  private final FnDataReceiver<WindowedValue<T>> consumer;
  private final Coder<WindowedValue<T>> coder;
  private final InboundDataClient readFuture;
  private long byteCounter;
  private long counter;

  public BeamFnDataInboundObserver(
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> consumer,
      InboundDataClient readFuture) {
    this.coder = coder;
    this.consumer = consumer;
    this.readFuture = readFuture;
  }

  @Override
  public void accept(BeamFnApi.Elements.Data t) {
    if (readFuture.isDone()) {
      // Drop any incoming data if the stream processing has finished.
      return;
    }
    try {
      if (t.getData().isEmpty()) {
        LOG.debug("Closing stream for instruction {} and "
            + "target {} having consumed {} values {} bytes",
            t.getInstructionReference(),
            t.getTarget(),
            counter,
            byteCounter);
        readFuture.complete();
        return;
      }

      byteCounter += t.getData().size();
      InputStream inputStream = t.getData().newInput();
      while (inputStream.available() > 0) {
        counter += 1;
        WindowedValue<T> value = coder.decode(inputStream);
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
