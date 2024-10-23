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
package org.apache.beam.runners.flink.streaming;

import java.util.Collection;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

class MemoryStateBackendWrapper {
  static <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
      Environment env,
      JobID jobID,
      String operatorIdentifier,
      TypeSerializer<K> keySerializer,
      int numberOfKeyGroups,
      KeyGroupRange keyGroupRange,
      TaskKvStateRegistry kvStateRegistry,
      TtlTimeProvider ttlTimeProvider,
      MetricGroup metricGroup,
      Collection<KeyedStateHandle> stateHandles,
      CloseableRegistry cancelStreamRegistry)
      throws BackendBuildingException {

    MemoryStateBackend backend = new MemoryStateBackend();
    return backend.createKeyedStateBackend(
        env,
        jobID,
        operatorIdentifier,
        keySerializer,
        numberOfKeyGroups,
        keyGroupRange,
        kvStateRegistry,
        ttlTimeProvider,
        metricGroup,
        stateHandles,
        cancelStreamRegistry);
  }

  static OperatorStateBackend createOperatorStateBackend(
      Environment env,
      String operatorIdentifier,
      Collection<OperatorStateHandle> stateHandles,
      CloseableRegistry cancelStreamRegistry)
      throws Exception {
    MemoryStateBackend backend = new MemoryStateBackend();
    return backend.createOperatorStateBackend(
        env, operatorIdentifier, stateHandles, cancelStreamRegistry);
  }
}
