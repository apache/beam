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
package org.apache.beam.sdk.extensions.euphoria.executor.local.testkit;

import org.apache.beam.sdk.extensions.euphoria.core.executor.Executor;
import org.apache.beam.sdk.extensions.euphoria.executor.local.LocalExecutor;
import org.apache.beam.sdk.extensions.euphoria.executor.local.WatermarkTriggerScheduler;
import org.apache.beam.sdk.extensions.euphoria.operator.test.junit.ExecutorEnvironment;
import org.apache.beam.sdk.extensions.euphoria.operator.test.junit.ExecutorProvider;

/**
 * Executor provider used for testing.
 */
public interface LocalExecutorProvider extends ExecutorProvider {
  @Override
  default ExecutorEnvironment newExecutorEnvironment() throws Exception {
    LocalExecutor exec =
        new LocalExecutor()
            .setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler(500L));
    return new ExecutorEnvironment() {
      @Override
      public Executor getExecutor() {
        return exec;
      }

      @Override
      public void shutdown() throws Exception {
        exec.shutdown();
      }
    };
  }
}
