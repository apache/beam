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
import java.util.List;

/**
 * Receiver that forwards each input it receives to each of a list of output Receivers.
 * Additionally, it invokes output counters who track size information for elements passing through.
 */
public class OutputReceiver implements Receiver {
  private final List<Receiver> outputs = new ArrayList<>();
  private final HashMap<String, ElementCounter> outputCounters = new HashMap<>();

  /** Adds a new receiver that this OutputReceiver forwards to. */
  public void addOutput(Receiver receiver) {
    outputs.add(receiver);
  }

  public void addOutputCounter(ElementCounter outputCounter) {
    outputCounters.put(Integer.toString(outputCounters.size()), outputCounter);
  }

  public void addOutputCounter(String counterName, ElementCounter outputCounter) {
    outputCounters.put(counterName, outputCounter);
  }

  @Override
  public void process(Object elem) throws Exception {
    for (ElementCounter counter : outputCounters.values()) {
      counter.update(elem);
    }

    // Fan-out.
    for (Receiver out : outputs) {
      if (out != null) {
        out.process(elem);
      }
    }

    for (ElementCounter counter : outputCounters.values()) {
      counter.finishLazyUpdate(elem);
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
