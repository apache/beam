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
 * A ParDo mapping function.
 */
public class ParDoOperation extends ReceivingOperation {
  public final ParDoFn fn;

  public ParDoOperation(String operationName,
                        ParDoFn fn,
                        OutputReceiver[] outputReceivers,
                        String counterPrefix,
                        CounterSet.AddCounterMutator addCounterMutator,
                        StateSampler stateSampler) {
    super(operationName, outputReceivers,
          counterPrefix, addCounterMutator, stateSampler);
    this.fn = fn;
  }

  @Override
  public void start() throws Exception {
    try (StateSampler.ScopedState start =
        stateSampler.scopedState(startState)) {
      super.start();
      fn.startBundle(receivers);
    }
  }

  @Override
  public void process(Object elem) throws Exception {
    try (StateSampler.ScopedState process =
        stateSampler.scopedState(processState)) {
      checkStarted();
      fn.processElement(elem);
    }
  }

  @Override
  public void finish() throws Exception {
    try (StateSampler.ScopedState finish =
        stateSampler.scopedState(finishState)) {
      checkStarted();
      fn.finishBundle();
      super.finish();
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
