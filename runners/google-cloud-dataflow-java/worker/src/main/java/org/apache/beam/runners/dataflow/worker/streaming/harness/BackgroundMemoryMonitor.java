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

import com.google.auto.value.AutoValue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Monitors memory pressure on a background executor. May be used to throttle calls, blocking if
 * there is memory pressure.
 */
@AutoValue
public abstract class BackgroundMemoryMonitor {
  public static BackgroundMemoryMonitor create(MemoryMonitor memoryMonitor) {
    return new AutoValue_BackgroundMemoryMonitor(
        memoryMonitor,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("MemoryMonitor")
                .setPriority(Thread.MIN_PRIORITY)
                .build()));
  }

  abstract MemoryMonitor memoryMonitor();

  abstract ExecutorService executor();

  public void start() {
    executor().execute(memoryMonitor());
  }

  public void shutdown() {
    memoryMonitor().stop();
    executor().shutdown();
  }
}
