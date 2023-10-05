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

import java.io.Closeable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/** A flatten operation. */
public class FlattenOperation extends ReceivingOperation {
  public FlattenOperation(OutputReceiver[] receivers, OperationContext context) {
    super(receivers, context);
  }

  /** Invoked by tests. */
  @VisibleForTesting
  public FlattenOperation(OutputReceiver outputReceiver, OperationContext context) {
    this(new OutputReceiver[] {outputReceiver}, context);
  }

  @Override
  public void process(Object elem) throws Exception {
    try (Closeable scope = context.enterProcess()) {
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
