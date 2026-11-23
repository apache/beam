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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler;
import org.apache.beam.sdk.values.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Receiver that forwards each input it receives to each of a list of output Receivers.
 * Additionally, it invokes output counters who track size information for elements passing through.
 */
@SuppressWarnings("nullness")
public class OutputReceiver implements Receiver, FusedSegmentCompiler.CompiledNode {
  private static final ElementCounter[] EMPTY_COUNTERS = new ElementCounter[0];
  private static final Receiver[] EMPTY_RECEIVERS = new Receiver[0];

  private final List<Receiver> outputs = new ArrayList<>();
  private final HashMap<String, ElementCounter> outputCounters = new LinkedHashMap<>();

  // Cached array/single-element fast-path fields to avoid LinkedHashMap/ArrayList Iterator
  // allocations and loop frames on every fused edge.
  private @Nullable ElementCounter onlyCounter = null;
  private ElementCounter[] countersArray = EMPTY_COUNTERS;
  private @Nullable Receiver onlyOutput = null;
  private Receiver[] outputsArray = EMPTY_RECEIVERS;
  private boolean singlePath = true;
  private StageTrampoline.@Nullable Cut trampolineCut = null;

  void setTrampolineCut(StageTrampoline.@Nullable Cut trampolineCut) {
    this.trampolineCut = trampolineCut;
    rebuildCaches();
  }

  public boolean hasTrampolineCut() {
    return trampolineCut != null;
  }

  public boolean hasSimpleSinglePath() {
    return singlePath;
  }

  public @Nullable ElementCounter getOnlyCounter() {
    return onlyCounter;
  }

  /** Adds a new receiver that this OutputReceiver forwards to. */
  public void addOutput(Receiver receiver) {
    outputs.add(receiver);
    rebuildCaches();
  }

  public void addOutputCounter(ElementCounter outputCounter) {
    outputCounters.put(Integer.toString(outputCounters.size()), outputCounter);
    rebuildCaches();
  }

  public void addOutputCounter(String counterName, ElementCounter outputCounter) {
    outputCounters.put(counterName, outputCounter);
    rebuildCaches();
  }

  private void rebuildCaches() {
    countersArray = outputCounters.values().toArray(EMPTY_COUNTERS);
    outputsArray = outputs.toArray(EMPTY_RECEIVERS);
    onlyCounter = countersArray.length == 1 ? countersArray[0] : null;
    onlyOutput = outputsArray.length == 1 ? outputsArray[0] : null;
    singlePath = trampolineCut == null && countersArray.length <= 1 && outputsArray.length <= 1;
  }

  private static @Nullable Receiver unwrapPassThroughFlatten(@Nullable Receiver target) {
    Receiver cur = target;
    while (cur != null && cur.getClass() == FlattenOperation.class) {
      Receiver next = ((FlattenOperation) cur).passThroughReceiverIfStarted();
      if (next == null) {
        break;
      }
      cur = next;
    }
    return cur;
  }

  @Override
  public void acceptWindowed(WindowedValue<?> elem) throws Exception {
    process(elem);
  }

  @Override
  public void acceptRaw(@Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
      throws Exception {
    WindowedValue<?> out =
        (rawValue == templateElem.getValue()) ? templateElem : templateElem.withValue(rawValue);
    process(out);
  }

  @Override
  public void process(Object elem) throws Exception {
    if (singlePath) {
      ElementCounter counter = onlyCounter;
      if (counter != null) {
        counter.update(elem);
      }
      Receiver out = unwrapPassThroughFlatten(onlyOutput);
      if (out != null) {
        out.process(elem);
      }
      if (counter != null) {
        counter.finishLazyUpdate(elem);
      }
      return;
    }
    processMultiple(elem);
  }

  void processDirect(Object elem) throws Exception {
    if (countersArray.length <= 1 && outputsArray.length <= 1) {
      ElementCounter counter = onlyCounter;
      if (counter != null) {
        counter.update(elem);
      }
      Receiver out = unwrapPassThroughFlatten(onlyOutput);
      if (out != null) {
        out.process(elem);
      }
      if (counter != null) {
        counter.finishLazyUpdate(elem);
      }
      return;
    }
    processMultipleDirect(elem);
  }

  private void processMultiple(Object elem) throws Exception {
    StageTrampoline.Cut cut = this.trampolineCut;
    if (cut != null) {
      cut.enqueue(elem);
      return;
    }
    ElementCounter[] counters = countersArray;
    int numCounters = counters.length;
    for (int i = 0; i < numCounters; i++) {
      counters[i].update(elem);
    }

    Receiver[] outs = outputsArray;
    int numOutputs = outs.length;
    for (int i = 0; i < numOutputs; i++) {
      Receiver out = unwrapPassThroughFlatten(outs[i]);
      if (out != null) {
        out.process(elem);
      }
    }

    for (int i = 0; i < numCounters; i++) {
      counters[i].finishLazyUpdate(elem);
    }
  }

  private void processMultipleDirect(Object elem) throws Exception {
    ElementCounter[] counters = countersArray;
    int numCounters = counters.length;
    for (int i = 0; i < numCounters; i++) {
      counters[i].update(elem);
    }

    Receiver[] outs = outputsArray;
    int numOutputs = outs.length;
    for (int i = 0; i < numOutputs; i++) {
      Receiver out = unwrapPassThroughFlatten(outs[i]);
      if (out != null) {
        out.process(elem);
      }
    }

    for (int i = 0; i < numCounters; i++) {
      counters[i].finishLazyUpdate(elem);
    }
  }

  public HashMap<String, ElementCounter> getOutputCounters() {
    return this.outputCounters;
  }

  /** Invoked by tests only. */
  public int getReceiverCount() {
    return outputs.size();
  }

  /** Invoked by tests only. */
  public Receiver getOnlyReceiver() {
    if (outputs.size() != 1) {
      throw new AssertionError("only one receiver expected");
    }

    return outputs.get(0);
  }
}
