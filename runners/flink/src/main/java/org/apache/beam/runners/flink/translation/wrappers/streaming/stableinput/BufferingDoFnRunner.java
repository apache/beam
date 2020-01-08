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
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.joda.time.Instant;

/**
 * A {@link DoFnRunner} which buffers data for supporting {@link
 * org.apache.beam.sdk.transforms.DoFn.RequiresStableInput}.
 *
 * <p>When a DoFn is annotated with @RequiresStableInput we are only allowed to process elements
 * after a checkpoint has completed. This ensures that the input is stable and we produce idempotent
 * results on failures.
 */
public class BufferingDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  public static <InputT, OutputT> BufferingDoFnRunner<InputT, OutputT> create(
      DoFnRunner<InputT, OutputT> doFnRunner,
      String stateName,
      org.apache.beam.sdk.coders.Coder windowedInputCoder,
      org.apache.beam.sdk.coders.Coder windowCoder,
      OperatorStateBackend operatorStateBackend,
      @Nullable KeyedStateBackend<Object> keyedStateBackend)
      throws Exception {
    return new BufferingDoFnRunner<>(
        doFnRunner,
        stateName,
        windowedInputCoder,
        windowCoder,
        operatorStateBackend,
        keyedStateBackend);
  }

  /** The underlying DoFnRunner that any buffered data will be handed over to eventually. */
  private final DoFnRunner<InputT, OutputT> underlying;
  /** A union list state which contains all to-be-acknowledged snapshot ids. */
  private final ListState<CheckpointElement> notYetAcknowledgedSnapshots;
  /** A factory for constructing new BufferingElementsHandler scoped by an internal id. */
  private final BufferingElementsHandlerFactory bufferingElementsHandlerFactory;

  /** The current active state id which is later linked to a checkpoint id. */
  private String currentStateId;
  /** The current handler used for buffering. */
  private BufferingElementsHandler currentBufferingElementsHandler;

  private BufferingDoFnRunner(
      DoFnRunner<InputT, OutputT> underlying,
      String stateName,
      org.apache.beam.sdk.coders.Coder inputCoder,
      org.apache.beam.sdk.coders.Coder windowCoder,
      OperatorStateBackend operatorStateBackend,
      @Nullable KeyedStateBackend keyedStateBackend)
      throws Exception {

    this.underlying = underlying;
    this.notYetAcknowledgedSnapshots =
        operatorStateBackend.getUnionListState(
            new ListStateDescriptor<>("notYetAcknowledgedSnapshots", CheckpointElement.class));
    this.bufferingElementsHandlerFactory =
        (stateId) -> {
          ListStateDescriptor<BufferedElement> stateDescriptor =
              new ListStateDescriptor<>(
                  stateName + stateId,
                  new CoderTypeSerializer<>(new BufferedElements.Coder(inputCoder, windowCoder)));
          if (keyedStateBackend != null) {
            return KeyedBufferingElementsHandler.create(keyedStateBackend, stateDescriptor);
          } else {
            return NonKeyedBufferingElementsHandler.create(
                operatorStateBackend.getListState(stateDescriptor));
          }
        };
    this.currentStateId = generateNewId();
    this.currentBufferingElementsHandler = bufferingElementsHandlerFactory.get(currentStateId);
  }

  @Override
  public void startBundle() {
    // Do not start a bundle, start it later when emitting elements
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    currentBufferingElementsHandler.buffer(new BufferedElements.Element(elem));
  }

  @Override
  public void onTimer(
      String timerId,
      String timerFamilyId,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    currentBufferingElementsHandler.buffer(
        new BufferedElements.Timer(
            timerId, timerFamilyId, window, timestamp, outputTimestamp, timeDomain));
  }

  @Override
  public void finishBundle() {
    // Do not finish a bundle, finish it later when emitting elements
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return underlying.getFn();
  }

  /** Should be called when a checkpoint is created. */
  public void checkpoint(long checkpointId) throws Exception {
    // We are about to get checkpointed. The elements buffered thus far
    // have to be added to the global CheckpointElement state which will
    // be used to emit elements later when this checkpoint is acknowledged.
    addToBeAcknowledgedCheckpoint(checkpointId, currentStateId);
    currentStateId = generateNewId();
    currentBufferingElementsHandler = bufferingElementsHandlerFactory.get(currentStateId);
  }

  /** Should be called when a checkpoint is completed. */
  public void checkpointCompleted(long checkpointId) throws Exception {
    List<CheckpointElement> toAck = removeToBeAcknowledgedCheckpoints(checkpointId);
    for (CheckpointElement toBeAcked : toAck) {
      BufferingElementsHandler bufferingElementsHandler =
          bufferingElementsHandlerFactory.get(toBeAcked.internalId);
      Iterator<BufferedElement> iterator = bufferingElementsHandler.getElements().iterator();
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

  private void addToBeAcknowledgedCheckpoint(long checkpointId, String internalId)
      throws Exception {
    notYetAcknowledgedSnapshots.addAll(
        Collections.singletonList(new CheckpointElement(internalId, checkpointId)));
  }

  private List<CheckpointElement> removeToBeAcknowledgedCheckpoints(long checkpointId)
      throws Exception {
    List<CheckpointElement> toBeAcknowledged = new ArrayList<>();
    List<CheckpointElement> checkpoints = new ArrayList<>();
    for (CheckpointElement element : notYetAcknowledgedSnapshots.get()) {
      if (element.checkpointId <= checkpointId) {
        toBeAcknowledged.add(element);
      } else {
        checkpoints.add(element);
      }
    }
    notYetAcknowledgedSnapshots.update(checkpoints);
    // Sort by checkpoint id to preserve order
    toBeAcknowledged.sort(Comparator.comparingLong(o -> o.checkpointId));
    return toBeAcknowledged;
  }

  private static String generateNewId() {
    return UUID.randomUUID().toString();
  }

  /** Constructs a new instance of BufferingElementsHandler with a provided state namespace. */
  private interface BufferingElementsHandlerFactory {
    BufferingElementsHandler get(String stateId) throws Exception;
  }

  private static class CheckpointElement {

    final String internalId;
    final long checkpointId;

    CheckpointElement(String internalId, long checkpointId) {
      this.internalId = internalId;
      this.checkpointId = checkpointId;
    }
  }
}
