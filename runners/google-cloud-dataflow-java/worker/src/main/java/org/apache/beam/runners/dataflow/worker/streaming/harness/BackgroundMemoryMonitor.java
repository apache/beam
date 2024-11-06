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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Monitors memory pressure on a background executor. May be used to throttle calls, blocking if
 * there is memory pressure.
 */
@Internal
@ThreadSafe
public final class BackgroundMemoryMonitor {
  private static final String MEMORY_MONITOR_THREAD_NAME = "MemoryMonitor";
  private final MemoryMonitor memoryMonitor;
  private final ExecutorService executor;

  private BackgroundMemoryMonitor(MemoryMonitor memoryMonitor, ExecutorService executor) {
    this.memoryMonitor = memoryMonitor;
    this.executor = executor;
  }

  public static BackgroundMemoryMonitor create(MemoryMonitor memoryMonitor) {
    return new BackgroundMemoryMonitor(
        memoryMonitor,
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(MEMORY_MONITOR_THREAD_NAME)
                .setPriority(Thread.MIN_PRIORITY)
                .build()));
  }

  public void start() {
    executor.execute(memoryMonitor);
  }

  public void shutdown() {
    memoryMonitor.stop();
    executor.shutdown();
  }
}
