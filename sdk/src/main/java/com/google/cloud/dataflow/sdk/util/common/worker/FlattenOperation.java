/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import com.google.cloud.dataflow.sdk.util.common.CounterSet;

/**
 * A flatten operation.
 */
public class FlattenOperation extends ReceivingOperation {
  public FlattenOperation(String operationName,
                          OutputReceiver[] receivers,
                          String counterPrefix,
                          CounterSet.AddCounterMutator addCounterMutator,
                          StateSampler stateSampler) {
    super(operationName, receivers,
          counterPrefix, addCounterMutator, stateSampler);
  }

  /** Invoked by tests. */
  public FlattenOperation(OutputReceiver outputReceiver,
                          String counterPrefix,
                          CounterSet.AddCounterMutator addCounterMutator,
                          StateSampler stateSampler) {
    this("FlattenOperation", new OutputReceiver[]{ outputReceiver },
         counterPrefix, addCounterMutator, stateSampler);
  }

  @Override
  public void process(Object elem) throws Exception {
    try (StateSampler.ScopedState process =
        stateSampler.scopedState(processState)) {
      checkStarted();
      Receiver receiver = receivers[0];
      if (receiver != null) {
        receiver.process(elem);
      }
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
