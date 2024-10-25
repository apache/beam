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
package org.apache.beam.runners.dataflow.worker.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.apache.beam.runners.dataflow.worker.WorkerUncaughtExceptionHandler;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;

/**
 * Utility class for {@link java.util.concurrent.ExecutorService}s that will terminate the JVM on
 * uncaught exceptions.
 *
 * @implNote Ensures that all threads produced by the {@link ExecutorService}s have a {@link
 *     WorkerUncaughtExceptionHandler} attached to prevent hidden/silent exceptions and errors.
 */
public final class TerminatingExecutors {
  private TerminatingExecutors() {}

  public static ExecutorService newSingleThreadedExecutor(
      ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return Executors.newSingleThreadExecutor(
        terminatingThreadFactory(threadFactoryBuilder, logger));
  }

  public static ScheduledExecutorService newSingleThreadedScheduledExecutor(
      ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return Executors.newSingleThreadScheduledExecutor(
        terminatingThreadFactory(threadFactoryBuilder, logger));
  }

  public static ExecutorService newCachedThreadPool(
      ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return Executors.newCachedThreadPool(terminatingThreadFactory(threadFactoryBuilder, logger));
  }

  public static ExecutorService newFixedThreadPool(
      int numThreads, ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return Executors.newFixedThreadPool(
        numThreads, terminatingThreadFactory(threadFactoryBuilder, logger));
  }

  private static ThreadFactory terminatingThreadFactory(
      ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return threadFactoryBuilder
        .setUncaughtExceptionHandler(new WorkerUncaughtExceptionHandler(logger))
        .build();
  }
}
