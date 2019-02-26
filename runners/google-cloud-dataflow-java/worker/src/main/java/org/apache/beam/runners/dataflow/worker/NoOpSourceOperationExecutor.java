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
package org.apache.beam.runners.dataflow.worker;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.api.services.dataflow.model.SourceSplitRequest;
import com.google.api.services.dataflow.model.SourceSplitResponse;
import java.util.Collections;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;

/** An executor for a source operation which does not perform any splits. */
public class NoOpSourceOperationExecutor implements SourceOperationExecutor {

  private final SourceOperationRequest request;
  private final CounterSet counterSet = new CounterSet();
  private SourceOperationResponse response;

  public NoOpSourceOperationExecutor(SourceOperationRequest request) {
    this.request = request;
  }

  @Override
  public CounterSet getOutputCounters() {
    return counterSet;
  }

  @Override
  public SourceOperationResponse getResponse() {
    return response;
  }

  @Override
  public void execute() throws Exception {
    SourceSplitRequest split = request.getSplit();
    if (split != null) {
      this.response =
          new SourceOperationResponse()
              .setSplit(new SourceSplitResponse().setOutcome("SOURCE_SPLIT_OUTCOME_USE_CURRENT"));
    } else {
      throw new UnsupportedOperationException("Unsupported source operation request: " + request);
    }
  }

  @Override
  public Iterable<CounterUpdate> extractMetricUpdates() {
    return Collections.emptyList();
  }
}
