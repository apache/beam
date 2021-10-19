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
package org.apache.beam.fn.harness.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.Data;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FnDataMultiplexer implements BeamFnDataClient {
  private static final Logger LOG = LoggerFactory.getLogger(FnDataMultiplexer.class);
  private Map<LogicalEndpoint, FnDataReceiver<ByteString>> registeredReceiver;
  private Map<LogicalEndpoint, FnDataInboundClient> inboundClients;
  private final List<Data> input;
  private final Consumer<Data> inlineConsumer;
  private final CloseableFnDataReceiver<?> streamDataReceiver;

  public FnDataMultiplexer(
      List<Data> input,
      Consumer<Data> inlineConsumer,
      CloseableFnDataReceiver<?> streamDataReceiver) {
    this.input = input;
    this.inlineConsumer = inlineConsumer;
    this.streamDataReceiver = streamDataReceiver;
    this.registeredReceiver = new ConcurrentHashMap<>();
    this.inboundClients = new ConcurrentHashMap<>();
  }

  public void dispatchData() throws Exception {
    LOG.info("Dispatching data of size {}", this.input.size());
    for (Elements.Data data : this.input) {
      String instructionId = data.getInstructionId();
      String transformId = data.getTransformId();
      LOG.info("Sending data of {}:{}", instructionId, transformId);
      if (instructionId != null && transformId != null) {
        registeredReceiver
            .get(LogicalEndpoint.data(instructionId, transformId))
            .accept(data.getData());
      } else {
        throw new IllegalArgumentException("Unknown transform");
      }
    }
    for (Elements.Data data : this.input) {
      String instructionId = data.getInstructionId();
      String transformId = data.getTransformId();

      if (instructionId != null && transformId != null) {
        if (!inboundClients.get(LogicalEndpoint.data(instructionId, transformId)).isDone()) {
          inboundClients.get(LogicalEndpoint.data(instructionId, transformId)).complete();
        }
      }
    }
  }

  @Override
  public InboundDataClient receive(
      ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint inputLocation,
      FnDataReceiver<ByteString> receiver) {
    registeredReceiver.put(inputLocation, receiver);
    return inboundClients.computeIfAbsent(inputLocation, x -> new FnDataInboundClient());
  }

  @Override
  public <T> CloseableFnDataReceiver<T> send(
      ApiServiceDescriptor apiServiceDescriptor, LogicalEndpoint outputLocation, Coder<T> coder) {
    return new FnDataOutputClient<>(
        outputLocation, coder, inlineConsumer, (CloseableFnDataReceiver<T>) streamDataReceiver);
  }

  // @Override
  // public CloseableFnDataReceiver<T> send(
  //     ApiServiceDescriptor apiServiceDescriptor, LogicalEndpoint outputLocation, Coder<T> coder)
  // {
  //   return new FnDataOutputClient<>(
  //       outputLocation, coder, inlineConsumer, (CloseableFnDataReceiver<T>) streamDataReceiver);
  // }

  public static class FnDataInboundClient implements InboundDataClient {
    private static final Object DONE = new Object();
    CompletableFuture<Object> done;

    public FnDataInboundClient() {
      done = new CompletableFuture<>();
    }

    @Override
    public void awaitCompletion() throws InterruptedException, Exception {
      done.get();
    }

    @Override
    public void runWhenComplete(Runnable completeRunnable) {}

    @Override
    public boolean isDone() {
      return done.isDone();
    }

    @Override
    public void cancel() {
      done.cancel(true);
    }

    @Override
    public void complete() {
      done.complete(DONE);
    }

    @Override
    public void fail(Throwable t) {
      done.completeExceptionally(t);
    }
  }

  public static class FnDataOutputClient<T> implements CloseableFnDataReceiver<T> {
    private final LogicalEndpoint outputLocation;
    private final Coder<T> coder;
    private final Consumer<Data> inlineConsumer;
    private final CloseableFnDataReceiver<T> streamDataReceiver;
    private final ByteString.Output bufferedOutput;
    private final List<T> bufferedElements;
    private boolean useStreamConsumer = false;

    public FnDataOutputClient(
        LogicalEndpoint outputLocation,
        Coder<T> coder,
        Consumer<Data> inlineConsumer,
        CloseableFnDataReceiver<T> streamDataReceiver) {
      this.outputLocation = outputLocation;
      this.coder = coder;
      this.inlineConsumer = inlineConsumer;
      this.streamDataReceiver = streamDataReceiver;
      this.bufferedOutput = ByteString.newOutput();
      this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void flush() throws Exception {
      if (useStreamConsumer) {
        streamDataReceiver.flush();
      }
    }

    @Override
    public void close() throws Exception {
      if (!useStreamConsumer) {
        Elements.Builder builder = Elements.newBuilder();
        builder
            .addDataBuilder()
            .setInstructionId(outputLocation.getInstructionId())
            .setTransformId(outputLocation.getTransformId())
            .setData(bufferedOutput.toByteString());
        for (Data data : builder.build().getDataList()) {
          inlineConsumer.accept(data);
        }
      }
      this.streamDataReceiver.close();
      this.bufferedOutput.close();
    }

    @Override
    public void accept(T input) throws Exception {
      if (useStreamConsumer) {
        streamDataReceiver.accept(input);
        return;
      }
      // buffered elements not large enough to switch to use stread data receiver.
      bufferedElements.add(input);
      // TODO: avoid encoding buffered output here.
      coder.encode(input, bufferedOutput);
      // if exceeded 1 mb, switch to stream consumer instead
      if (bufferedOutput.size() > 1024 * 1024) {
        useStreamConsumer = true;
        for (T element : bufferedElements) {
          streamDataReceiver.accept(element);
        }
        LOG.info(
            "FnHarness sending data through stream consumer because buffered data size is now {}",
            bufferedOutput.size());
        bufferedOutput.close();
        bufferedElements.clear();
      }
    }
  }
}
