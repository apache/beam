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
package org.apache.beam.fn.harness.status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessorCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p36p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p36p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamFnStatusClient {
  private final StreamObserver<WorkerStatusResponse> outboundObserver;
  private final BundleProcessorCache processBundleCache;
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnStatusClient.class);
  private final MemoryMonitor memoryMonitor;

  public BeamFnStatusClient(
      ApiServiceDescriptor apiServiceDescriptor,
      Function<ApiServiceDescriptor, ManagedChannel> channelFactory,
      BundleProcessorCache processBundleCache,
      PipelineOptions options) {
    BeamFnWorkerStatusGrpc.BeamFnWorkerStatusStub stub =
        BeamFnWorkerStatusGrpc.newStub(channelFactory.apply(apiServiceDescriptor));
    stub.workerStatus(new InboundObserver());
    this.outboundObserver = stub.workerStatus(new InboundObserver());
    this.processBundleCache = processBundleCache;
    this.memoryMonitor = MemoryMonitor.fromOptions(options);
    Thread thread = new Thread(memoryMonitor);
    thread.setDaemon(true);
    thread.setPriority(Thread.MIN_PRIORITY);
    thread.setName("MemoryMonitor");
    thread.start();
  }

  /**
   * Class representing the execution state of a thread.
   *
   * <p>Can be used in hash maps.
   */
  static class Stack {
    final StackTraceElement[] elements;
    final Thread.State state;

    Stack(StackTraceElement[] elements, Thread.State state) {
      this.elements = elements;
      this.state = state;
    }

    @Override
    public int hashCode() {
      return Objects.hash(Arrays.deepHashCode(elements), state);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == this) {
        return true;
      } else if (!(other instanceof Stack)) {
        return false;
      } else {
        Stack that = (Stack) other;
        return state == that.state && Arrays.deepEquals(elements, that.elements);
      }
    }
  }

  String getThreadDump() {
    StringJoiner trace = new StringJoiner("\n");
    trace.add("========== THREAD DUMP ==========");
    // filter duplicates.
    Map<Stack, List<String>> stacks = new HashMap<>();
    Thread.getAllStackTraces()
        .forEach(
            (thread, elements) -> {
              if (thread != Thread.currentThread()) {
                Stack stack = new Stack(elements, thread.getState());
                stacks.putIfAbsent(stack, new ArrayList<>());
                stacks.get(stack).add(thread.toString());
              }
            });

    // Stacks with more threads are printed first.
    stacks.entrySet().stream()
        .sorted(Comparator.comparingInt(entry -> -entry.getValue().size()))
        .forEachOrdered(
            entry -> {
              Stack stack = entry.getKey();
              List<String> threads = entry.getValue();
              trace.add(
                  String.format(
                      "---- Threads (%d): %s State: %s Stack: ----",
                      threads.size(), threads, stack.state));
              Arrays.stream(stack.elements).map(StackTraceElement::toString).forEach(trace::add);
              trace.add("\n");
            });
    return trace.toString();
  }

  String getMemoryUsage() {
    StringJoiner memory = new StringJoiner("\n");
    memory.add("========== MEMORY USAGE ==========");
    memory.add(memoryMonitor.describeMemory());
    return memory.toString();
  }

  @VisibleForTesting
  String getActiveProcessBundleState() {
    StringJoiner activeBundlesState = new StringJoiner("\n");
    activeBundlesState.add("========== ACTIVE PROCESSING BUNDLES ==========");
    if (processBundleCache.getActiveBundleProcessors().isEmpty()) {
      activeBundlesState.add("No active processing bundles.");
    } else {
      processBundleCache.getActiveBundleProcessors().entrySet().stream()
          .sorted(
              Comparator.comparingLong(
                      (Map.Entry<String, BundleProcessor> bundle) ->
                          bundle.getValue().getStateTracker().getMillisSinceLastTransition())
                  .reversed()) // reverse sort active bundle by time since last transition.
          .limit(10) // only keep top 10
          .forEach(
              entry -> {
                ExecutionStateTracker executionStateTracker = entry.getValue().getStateTracker();
                activeBundlesState.add(String.format("---- Instruction %s ----", entry.getKey()));
                activeBundlesState.add(
                    String.format(
                        "Tracked thread: %s", executionStateTracker.getTrackedThread().getName()));
                activeBundlesState.add(
                    String.format(
                        "Time since transition: %.2f seconds%n",
                        executionStateTracker.getMillisSinceLastTransition() / 1000.0));
              });
    }
    return activeBundlesState.toString();
  }

  private class InboundObserver implements StreamObserver<BeamFnApi.WorkerStatusRequest> {
    @Override
    public void onNext(WorkerStatusRequest workerStatusRequest) {
      StringJoiner status = new StringJoiner("\n");
      status.add(getMemoryUsage());
      status.add("\n");
      status.add(getActiveProcessBundleState());
      status.add("\n");
      status.add(getThreadDump());
      outboundObserver.onNext(
          WorkerStatusResponse.newBuilder()
              .setId(workerStatusRequest.getId())
              .setStatusInfo(status.toString())
              .build());
    }

    @Override
    public void onError(Throwable t) {
      LOG.error("Error getting SDK harness status", t);
      outboundObserver.onError(t);
    }

    @Override
    public void onCompleted() {}
  }
}
