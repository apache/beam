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
package org.apache.beam.runners.mapreduce.translation;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Class that processes elements and forwards outputs to consumers.
 */
public abstract class Operation<T> implements Serializable {
  private final OutputReceiver[] receivers;

  public Operation(int numOutputs) {
    this.receivers = new OutputReceiver[numOutputs];
    for (int i = 0; i < numOutputs; ++i) {
      receivers[i] = new OutputReceiver();
    }
  }

  /**
   * Starts this Operation's execution.
   *
   * <p>Called after all successors consuming operations have been started.
   */
  public void start(TaskInputOutputContext<Object, Object, Object, Object> taskContext) {
    for (OutputReceiver receiver : receivers) {
      if (receiver == null) {
        continue;
      }
      for (Operation operation : receiver.getReceivingOperations()) {
        operation.start(taskContext);
      }
    }
  }

  /**
   * Processes the element.
   */
  public abstract void process(WindowedValue<T> elem) throws IOException, InterruptedException;

  /**
   * Finishes this Operation's execution.
   *
   * <p>Called after all predecessors producing operations have been finished.
   */
  public void finish() {
    for (OutputReceiver receiver : receivers) {
      if (receiver == null) {
        continue;
      }
      for (Operation operation : receiver.getReceivingOperations()) {
        operation.finish();
      }
    }
  }

  public List<OutputReceiver> getOutputReceivers() {
    return ImmutableList.copyOf(receivers);
  }

  /**
   * Adds an output to this Operation.
   */
  public void attachConsumer(TupleTag<?> tupleTag, Operation consumer) {
    int outputIndex = getOutputIndex(tupleTag);
    OutputReceiver fanOut = receivers[outputIndex];
    fanOut.addOutput(consumer);
  }

  protected int getOutputIndex(TupleTag<?> tupleTag) {
    return 0;
  }
}
