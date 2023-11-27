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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.io.IOException;
import java.util.concurrent.Future;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;

/**
 * Base class for implementations of {@link WindmillState} where the {@link #persist} call does not
 * require any asynchronous reading.
 */
abstract class SimpleWindmillState extends WindmillState {
  @Override
  public final Future<Windmill.WorkItemCommitRequest> persist(
      WindmillStateCache.ForKeyAndFamily cache) throws IOException {
    return Futures.immediateFuture(persistDirectly(cache));
  }

  /**
   * Returns a {@link Windmill.WorkItemCommitRequest} that can be used to persist this state to
   * Windmill.
   */
  protected abstract Windmill.WorkItemCommitRequest persistDirectly(
      WindmillStateCache.ForKeyAndFamily cache) throws IOException;
}
