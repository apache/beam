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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.StreamingEngineThrottleTimers;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;

/**
 * Owns and maintains a set of streams used to communicate with a specific Windmill worker.
 * Underlying streams are "cached" in a threadsafe manner so that once {@link Supplier#get} is
 * called, a stream that is already started is returned.
 *
 * <p>Holds references to {@link
 * Supplier<org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream>} because
 * initializing the streams automatically start them, and we want to do so lazily here once the
 * {@link GetWorkBudget} is set.
 *
 * <p>Once started, the underlying streams are "alive" until they are manually closed via {@link
 * #closeAllStreams()}.
 *
 * <p>If closed, it means that the backend endpoint is no longer in the worker set. Once closed,
 * these instances are not reused.
 *
 * @implNote Does not manage streams for fetching {@link
 *     org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData} for side inputs.
 */
@Internal
@ThreadSafe
public class WindmillStreamSender {
  private final AtomicBoolean started;
  private final AtomicReference<GetWorkBudget> getWorkBudget;
  private final Supplier<GetWorkStream> getWorkStream;
  private final Supplier<GetDataStream> getDataStream;
  private final Supplier<CommitWorkStream> commitWorkStream;
  private final StreamingEngineThrottleTimers streamingEngineThrottleTimers;

  private WindmillStreamSender(
      CloudWindmillServiceV1Alpha1Stub stub,
      GetWorkRequest getWorkRequest,
      AtomicReference<GetWorkBudget> getWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemProcessor workItemProcessor) {
    this.started = new AtomicBoolean(false);
    this.getWorkBudget = getWorkBudget;
    this.streamingEngineThrottleTimers = StreamingEngineThrottleTimers.create();

    // All streams are memoized/cached since they are expensive to create and some implementations
    // perform side effects on construction (i.e. sending initial requests to the stream server to
    // initiate the streaming RPC connection). Stream instances connect/reconnect internally so we
    // can reuse the same instance through the entire lifecycle of WindmillStreamSender.
    this.getDataStream =
        Suppliers.memoize(
            () ->
                streamingEngineStreamFactory.createGetDataStream(
                    stub, streamingEngineThrottleTimers.getDataThrottleTimer()));
    this.commitWorkStream =
        Suppliers.memoize(
            () ->
                streamingEngineStreamFactory.createCommitWorkStream(
                    stub, streamingEngineThrottleTimers.commitWorkThrottleTimer()));
    this.getWorkStream =
        Suppliers.memoize(
            () ->
                streamingEngineStreamFactory.createDirectGetWorkStream(
                    stub,
                    withRequestBudget(getWorkRequest, getWorkBudget.get()),
                    streamingEngineThrottleTimers.getWorkThrottleTimer(),
                    getDataStream,
                    commitWorkStream,
                    workItemProcessor));
  }

  public static WindmillStreamSender create(
      CloudWindmillServiceV1Alpha1Stub stub,
      GetWorkRequest getWorkRequest,
      GetWorkBudget getWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemProcessor workItemProcessor) {
    return new WindmillStreamSender(
        stub,
        getWorkRequest,
        new AtomicReference<>(getWorkBudget),
        streamingEngineStreamFactory,
        workItemProcessor);
  }

  private static GetWorkRequest withRequestBudget(GetWorkRequest request, GetWorkBudget budget) {
    return request.toBuilder().setMaxItems(budget.items()).setMaxBytes(budget.bytes()).build();
  }

  @SuppressWarnings("ReturnValueIgnored")
  void startStreams() {
    getWorkStream.get();
    getDataStream.get();
    commitWorkStream.get();
    // *stream.get() is all memoized in a threadsafe manner.
    started.set(true);
  }

  void closeAllStreams() {
    // Supplier<Stream>.get() starts the stream which is an expensive operation as it initiates the
    // streaming RPCs by possibly making calls over the network. Do not close the streams unless
    // they have already been started.
    if (started.get()) {
      getWorkStream.get().close();
      getDataStream.get().close();
      commitWorkStream.get().close();
    }
  }

  public void adjustBudget(long itemsDelta, long bytesDelta) {
    getWorkBudget.set(getWorkBudget.get().apply(itemsDelta, bytesDelta));
    if (started.get()) {
      getWorkStream.get().adjustBudget(itemsDelta, bytesDelta);
    }
  }

  public void adjustBudget(GetWorkBudget adjustment) {
    adjustBudget(adjustment.items(), adjustment.bytes());
  }

  public GetWorkBudget remainingGetWorkBudget() {
    return started.get() ? getWorkStream.get().remainingBudget() : getWorkBudget.get();
  }

  public long getAndResetThrottleTime() {
    return streamingEngineThrottleTimers.getAndResetThrottleTime();
  }
}
