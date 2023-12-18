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
package org.apache.beam.runners.flink.translation.wrappers.streaming.stableinput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.utils.Locker;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link DoFnRunner} which buffers data for supporting {@link
 * org.apache.beam.sdk.transforms.DoFn.RequiresStableInput}.
 *
 * <p>When a DoFn is annotated with @RequiresStableInput we are only allowed to process elements
 * after a checkpoint has completed. This ensures that the input is stable and we produce idempotent
 * results on failures.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BufferingDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  public static <InputT, OutputT> BufferingDoFnRunner<InputT, OutputT> create(
      DoFnRunner<InputT, OutputT> doFnRunner,
      String stateName,
      org.apache.beam.sdk.coders.Coder windowedInputCoder,
      org.apache.beam.sdk.coders.Coder windowCoder,
      OperatorStateBackend operatorStateBackend,
      @Nullable KeyedStateBackend<Object> keyedStateBackend,
      int maxConcurrentCheckpoints,
      SerializablePipelineOptions pipelineOptions)
      throws Exception {

    return new BufferingDoFnRunner<>(
        doFnRunner,
        stateName,
        windowedInputCoder,
        windowCoder,
        operatorStateBackend,
        maxConcurrentCheckpoints,
        pipelineOptions,
        keyedStateBackend,
        null,
        null,
        null);
  }

  public static <InputT, OutputT> BufferingDoFnRunner<InputT, OutputT> create(
      DoFnRunner<InputT, OutputT> doFnRunner,
      String stateName,
      org.apache.beam.sdk.coders.Coder windowedInputCoder,
      org.apache.beam.sdk.coders.Coder windowCoder,
      OperatorStateBackend operatorStateBackend,
      @Nullable KeyedStateBackend<Object> keyedStateBackend,
      int maxConcurrentCheckpoints,
      SerializablePipelineOptions pipelineOptions,
      @Nullable Supplier<Locker> locker,
      @Nullable Function<InputT, Object> keySelector,
      @Nullable Runnable finishBundleCallback)
      throws Exception {

    return new BufferingDoFnRunner<>(
        doFnRunner,
        stateName,
        windowedInputCoder,
        windowCoder,
        operatorStateBackend,
        maxConcurrentCheckpoints,
        pipelineOptions,
        keyedStateBackend,
        locker,
        keyedStateBackend != null ? keySelector : null,
        finishBundleCallback);
  }

  /** The underlying DoFnRunner that any buffered data will be handed over to eventually. */
  private final DoFnRunner<InputT, OutputT> underlying;
  /** A union list state which contains all to-be-acknowledged snapshot ids. */
  private final ListState<CheckpointIdentifier> notYetAcknowledgedSnapshots;
  /** A factory for constructing new BufferingElementsHandler scoped by an internal id. */
  private final BufferingElementsHandlerFactory bufferingElementsHandlerFactory;
  /** The maximum number of buffers for data of not yet acknowledged checkpoints. */
  final int numCheckpointBuffers;
  /** The current active state id which, on checkpoint, is linked to a checkpoint id. */
  int currentStateIndex;
  /** The current handler used for buffering. */
  private BufferingElementsHandler currentBufferingElementsHandler;
  /** Minimum timestamp of all buffered elements. */
  private volatile long minBufferedElementTimestamp;
  /** The associated keyed state backend. */
  private final @Nullable KeyedStateBackend keyedStateBackend;
  /**
   * Locker that must be held (if present) before buffering an element. If non-null, we must
   * manually set a key to the state backend.
   */
  private final @Nullable Supplier<Locker> locker;
  /**
   * A selector of key. When non-null, this must be set to the keyed state backend before buffering.
   */
  private final @Nullable Function<InputT, Object> keySelector;
  /** Callable to notify about possibility to flush bundle. */
  private final @Nullable Runnable finishBundleCallback;

  private BufferingDoFnRunner(
      DoFnRunner<InputT, OutputT> underlying,
      String stateName,
      org.apache.beam.sdk.coders.Coder inputCoder,
      org.apache.beam.sdk.coders.Coder windowCoder,
      OperatorStateBackend operatorStateBackend,
      int maxConcurrentCheckpoints,
      SerializablePipelineOptions pipelineOptions,
      @Nullable KeyedStateBackend keyedStateBackend,
      @Nullable Supplier<Locker> locker,
      @Nullable Function<InputT, Object> keySelector,
      @Nullable Runnable finishBundleCallback)
      throws Exception {

    Preconditions.checkArgument(
        maxConcurrentCheckpoints > 0 && maxConcurrentCheckpoints < Short.MAX_VALUE,
        "Maximum number of concurrent checkpoints not within the bounds of 0 and %s",
        Short.MAX_VALUE);

    this.underlying = underlying;
    this.notYetAcknowledgedSnapshots =
        operatorStateBackend.getUnionListState(
            new ListStateDescriptor<>("notYetAcknowledgedSnapshots", CheckpointIdentifier.class));
    this.bufferingElementsHandlerFactory =
        (stateId) -> {
          ListStateDescriptor<BufferedElement> stateDescriptor =
              new ListStateDescriptor<>(
                  stateName + stateId,
                  new CoderTypeSerializer<>(
                      new BufferedElements.Coder(inputCoder, windowCoder, null), pipelineOptions));
          if (keyedStateBackend != null) {
            return KeyedBufferingElementsHandler.create(keyedStateBackend, stateDescriptor);
          } else {
            return NonKeyedBufferingElementsHandler.create(
                operatorStateBackend.getListState(stateDescriptor));
          }
        };
    this.numCheckpointBuffers = initializeState(maxConcurrentCheckpoints);
    this.currentBufferingElementsHandler =
        bufferingElementsHandlerFactory.get(rotateAndGetStateIndex());
    this.keyedStateBackend = keyedStateBackend;
    this.locker = locker;
    this.keySelector = keySelector;
    this.finishBundleCallback = finishBundleCallback;

    Preconditions.checkArgument(
        keySelector == null || keyedStateBackend != null,
        "keySelector must be null for null keyed state backend");
  }

  /**
   * Initialize the state index and the max checkpoint buffers based on previous not yet
   * acknowledged checkpoints.
   */
  private int initializeState(int maxConcurrentCheckpoints) throws Exception {
    List<CheckpointIdentifier> pendingSnapshots = new ArrayList<>();
    Iterables.addAll(pendingSnapshots, notYetAcknowledgedSnapshots.get());
    int lastUsedIndex = -1;
    int maxIndex = 0;
    if (!pendingSnapshots.isEmpty()) {
      for (CheckpointIdentifier checkpointIdentifier : pendingSnapshots) {
        maxIndex = Math.max(maxIndex, checkpointIdentifier.internalId);
      }
      lastUsedIndex = pendingSnapshots.get(pendingSnapshots.size() - 1).internalId;
    }
    this.currentStateIndex = lastUsedIndex;
    this.minBufferedElementTimestamp = Long.MAX_VALUE;
    // If a previous run had a higher number of concurrent checkpoints we need to use this number to
    // not break the buffering/flushing logic.
    return Math.max(maxConcurrentCheckpoints, maxIndex) + 1;
  }

  @Override
  public void startBundle() {
    // Do not start a bundle, start it later when emitting elements
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    minBufferedElementTimestamp =
        Math.min(elem.getTimestamp().getMillis(), minBufferedElementTimestamp);
    try (Locker lock = locker != null ? locker.get() : null) {
      if (keySelector != null) {
        keyedStateBackend.setCurrentKey(keySelector.apply(elem.getValue()));
      }
      currentBufferingElementsHandler.buffer(new BufferedElements.Element(elem));
    }
  }

  @Override
  public <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {

    minBufferedElementTimestamp =
        Math.min(outputTimestamp.getMillis(), minBufferedElementTimestamp);
    try (Locker lock = locker != null ? locker.get() : null) {
      if (keySelector != null) {
        keyedStateBackend.setCurrentKey(key);
      }
      currentBufferingElementsHandler.buffer(
          new BufferedElements.Timer<>(
              timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain));
    }
  }

  @Override
  public void finishBundle() {
    Optional.ofNullable(finishBundleCallback).ifPresent(Runnable::run);
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {}

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return underlying.getFn();
  }

  /** Should be called when a checkpoint is created. */
  public void checkpoint(long checkpointId) throws Exception {
    // We are about to get checkpointed. The elements buffered thus far
    // have to be added to the global CheckpointElement state which will
    // be used to emit elements later when this checkpoint is acknowledged.
    addToBeAcknowledgedCheckpoint(checkpointId, getStateIndex());
    int newStateIndex = rotateAndGetStateIndex();
    currentBufferingElementsHandler = bufferingElementsHandlerFactory.get(newStateIndex);
  }

  /** Should be called when a checkpoint is completed. */
  public void checkpointCompleted(long checkpointId) throws Exception {
    List<CheckpointIdentifier> allToAck = gatherToBeAcknowledgedCheckpoints(checkpointId);
    for (CheckpointIdentifier toBeAcked : allToAck) {
      BufferingElementsHandler bufferingElementsHandler =
          bufferingElementsHandlerFactory.get(toBeAcked.internalId);
      try (Locker lock = locker != null ? locker.get() : null) {
        final Iterator<BufferedElement> iterator =
            bufferingElementsHandler.getElements().iterator();
        boolean hasElements = iterator.hasNext();
        if (hasElements) {
          underlying.startBundle();
        }
        while (iterator.hasNext()) {
          BufferedElement bufferedElement = iterator.next();
          bufferedElement.processWith(underlying);
        }
        if (hasElements) {
          underlying.finishBundle();
        }
        bufferingElementsHandler.clear();
      }
    }
    minBufferedElementTimestamp = Long.MAX_VALUE;
  }

  public long getOutputWatermarkHold() {
    return minBufferedElementTimestamp;
  }

  private void addToBeAcknowledgedCheckpoint(long checkpointId, int internalId) throws Exception {
    notYetAcknowledgedSnapshots.addAll(
        Collections.singletonList(new CheckpointIdentifier(internalId, checkpointId)));
  }

  private List<CheckpointIdentifier> gatherToBeAcknowledgedCheckpoints(long checkpointId)
      throws Exception {
    List<CheckpointIdentifier> toBeAcknowledged = new ArrayList<>();
    List<CheckpointIdentifier> remaining = new ArrayList<>();
    for (CheckpointIdentifier element : notYetAcknowledgedSnapshots.get()) {
      if (element.checkpointId <= checkpointId) {
        toBeAcknowledged.add(element);
      } else {
        remaining.add(element);
      }
    }
    notYetAcknowledgedSnapshots.update(remaining);
    // Sort by checkpoint id to preserve order
    toBeAcknowledged.sort(Comparator.comparingLong(o -> o.checkpointId));
    return toBeAcknowledged;
  }

  private int rotateAndGetStateIndex() {
    currentStateIndex = (currentStateIndex + 1) % numCheckpointBuffers;
    return currentStateIndex;
  }

  private int getStateIndex() {
    return currentStateIndex;
  }

  /** Constructs a new instance of BufferingElementsHandler with a provided state namespace. */
  private interface BufferingElementsHandlerFactory {
    BufferingElementsHandler get(int stateIndex) throws Exception;
  }

  static class CheckpointIdentifier {

    final int internalId;
    final long checkpointId;

    CheckpointIdentifier(int internalId, long checkpointId) {
      this.internalId = internalId;
      this.checkpointId = checkpointId;
    }
  }
}
