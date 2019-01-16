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
import org.apache.beam.runners.dataflow.worker.util.common.worker.WorkExecutor;

/** An general executor for work in the Dataflow legacy and Beam harness. */
public interface DataflowWorkExecutor extends WorkExecutor {

  /**
   * Extract metric updates that are still pending to be reported to the Dataflow service, removing
   * them from this {@link DataflowWorkExecutor}.
   */
  Iterable<CounterUpdate> extractMetricUpdates();
}
