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
package org.apache.beam.runners.dataflow;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;

/**
 * A service interface for receiving counter updates from the Dataflow runner.
 *
 * <p>During pipeline execution, the runner will periodically call {@link
 * CounterUpdateReceiver#receiverCounterUpdates(List)} with a list of updated counters.
 *
 * <p>{@link java.util.ServiceLoader} is used to discover implementations of {@link
 * CounterUpdateReceiver}, note that you will need to register your implementation with the
 * appropriate resources to ensure your code is executed. You can use a tool like {@link
 * com.google.auto.service.AutoService} to automate this.
 */
@Experimental
public interface CounterUpdateReceiver {

  /**
   * This method is called periodically by the Dataflow runner. See {@link CounterUpdate} for more
   * information on how to interpret the updates.
   *
   * @param updates A list of {@link CounterUpdate}
   */
  void receiverCounterUpdates(List<CounterUpdate> updates);
}
