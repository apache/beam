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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.sdk.annotations.Internal;

@Internal
class MetricsDataProvider implements StatusDataProvider {

  private final BoundedQueueExecutor workUnitExecutor;
  private final Supplier<Long> currentActiveCommitBytes;
  private final Consumer<PrintWriter> getDataStatusProvider;
  private final Supplier<Collection<ComputationState>> allComputationStates;

  MetricsDataProvider(
      BoundedQueueExecutor workUnitExecutor,
      Supplier<Long> currentActiveCommitBytes,
      Consumer<PrintWriter> getDataStatusProvider,
      Supplier<Collection<ComputationState>> allComputationStates) {
    this.workUnitExecutor = workUnitExecutor;
    this.currentActiveCommitBytes = currentActiveCommitBytes;
    this.getDataStatusProvider = getDataStatusProvider;
    this.allComputationStates = allComputationStates;
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.println(workUnitExecutor.summaryHtml());

    writer.print("Active commit: ");
    appendHumanizedBytes(currentActiveCommitBytes.get(), writer);
    writer.println("<br>");

    getDataStatusProvider.accept(writer);

    writer.println("<br>");

    writer.println("Active Keys: <br>");
    for (ComputationState computationState : allComputationStates.get()) {
      writer.print(computationState.getComputationId());
      writer.print(":<br>");
      computationState.printActiveWork(writer);
      writer.println("<br>");
    }
  }

  private void appendHumanizedBytes(long bytes, PrintWriter writer) {
    if (bytes < (4 << 10)) {
      writer.print(bytes);
      writer.print("B");
    } else if (bytes < (4 << 20)) {
      writer.print("~");
      writer.print(bytes >> 10);
      writer.print("KB");
    } else {
      writer.print("~");
      writer.print(bytes >> 20);
      writer.print("MB");
    }
  }
}
