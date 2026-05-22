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
package org.apache.beam.runners.dataflow.worker.streaming;

import com.google.auto.value.AutoValue;
import java.util.function.BiConsumer;
import org.apache.beam.runners.dataflow.worker.util.ExceptionUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;

/** {@link Work} instance and a processing function used to process the work. */
@AutoValue
public abstract class ExecutableWork {

  public static ExecutableWork create(
      Work work, BiConsumer<Work, BoundedQueueExecutorWorkHandle> executeWorkFn) {
    return new AutoValue_ExecutableWork(work, executeWorkFn);
  }

  public abstract Work work();

  public abstract BiConsumer<Work, BoundedQueueExecutorWorkHandle> executeWorkFn();

  public void run(BoundedQueueExecutorWorkHandle handle) {
    try {
      executeWorkFn().accept(work(), handle);
    } catch (Throwable t) {
      throw ExceptionUtils.propagate(t);
    }
  }

  public final WorkId id() {
    return work().id();
  }

  public final Windmill.WorkItem getWorkItem() {
    return work().getWorkItem();
  }
}
