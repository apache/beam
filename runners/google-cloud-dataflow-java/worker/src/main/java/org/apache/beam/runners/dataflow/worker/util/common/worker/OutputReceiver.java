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
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Receiver that forwards each input it receives to each of a list of output Receivers.
 * Additionally, it invokes output counters who track size information for elements passing through.
 */
public class OutputReceiver implements Receiver {
  private final List<@Nullable Receiver> outputs = new ArrayList<>(2);
  private final HashMap<String, ElementCounter> outputCounters = new LinkedHashMap<>(4);

  // Pre-resolved dispatch state maintained on addOutput / addOutputCounter so the dominant
  // 1-counter / 1-receiver hot path avoids iterator allocations, array indirections, and bounds
  // checks. When count == 0 both are null; when count == 1 single* is non-null; when count >= 2
  // multiple* holds the compact non-null snapshot array.
  private @Nullable Receiver singleOutput;
  private Receiver @Nullable [] multipleOutputs;
  private @Nullable ElementCounter singleCounter;
  private ElementCounter @Nullable [] multipleCounters;

  /** Adds a new receiver that this OutputReceiver forwards to. */
  public void addOutput(@Nullable Receiver receiver) {
    outputs.add(receiver);
    if (receiver == null) {
      return;
    }
    if (multipleOutputs != null) {
      int len = multipleOutputs.length;
      Receiver[] updated = new Receiver[len + 1];
      System.arraycopy(multipleOutputs, 0, updated, 0, len);
      updated[len] = receiver;
      multipleOutputs = updated;
    } else if (singleOutput == null) {
      singleOutput = receiver;
    } else {
      multipleOutputs = new Receiver[] {singleOutput, receiver};
      singleOutput = null;
    }
  }

  public void addOutputCounter(ElementCounter outputCounter) {
    addOutputCounter(Integer.toString(outputCounters.size()), outputCounter);
  }

  public void addOutputCounter(String counterName, ElementCounter outputCounter) {
    outputCounters.put(counterName, Objects.requireNonNull(outputCounter));
    int size = outputCounters.size();
    if (size == 1) {
      singleCounter = outputCounter;
      multipleCounters = null;
    } else {
      singleCounter = null;
      multipleCounters = outputCounters.values().toArray(new ElementCounter[0]);
    }
  }

  @Override
  public void process(Object elem) throws Exception {
    ElementCounter counter = this.singleCounter;
    ElementCounter @Nullable [] counters = null;
    if (counter != null) {
      counter.update(elem);
    } else {
      counters = this.multipleCounters;
      if (counters != null) {
        updateMultipleCounters(counters, elem);
      }
    }

    // Fan-out.
    Receiver out = this.singleOutput;
    if (out != null) {
      out.process(elem);
    } else {
      Receiver[] outs = this.multipleOutputs;
      if (outs != null) {
        processMultipleOutputs(outs, elem);
      }
    }

    if (counter != null) {
      counter.finishLazyUpdate(elem);
    } else if (counters != null) {
      finishLazyMultipleCounters(counters, elem);
    }
  }

  private static void updateMultipleCounters(ElementCounter[] counters, Object elem)
      throws Exception {
    for (int i = 0; i < counters.length; i++) {
      counters[i].update(elem);
    }
  }

  private static void processMultipleOutputs(Receiver[] outs, Object elem) throws Exception {
    for (int i = 0; i < outs.length; i++) {
      outs[i].process(elem);
    }
  }

  private static void finishLazyMultipleCounters(ElementCounter[] counters, Object elem) {
    for (int i = 0; i < counters.length; i++) {
      counters[i].finishLazyUpdate(elem);
    }
  }

  /**
   * Returns a snapshot copy of the registered output counters keyed by counter name. Callers must
   * use {@link #addOutputCounter} to register new counters; mutating the returned map does not
   * affect this receiver.
   */
  public HashMap<String, ElementCounter> getOutputCounters() {
    return new LinkedHashMap<>(this.outputCounters);
  }

  /** Invoked by tests only. */
  public int getReceiverCount() {
    return outputs.size();
  }

  /** Invoked by tests only. */
  public @Nullable Receiver getOnlyReceiver() {
    if (outputs.size() != 1) {
      throw new AssertionError("only one receiver expected");
    }

    return outputs.get(0);
  }
}
