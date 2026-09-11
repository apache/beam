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
package org.apache.beam.runners.spark.structuredstreaming;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.sdk.PipelineResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SparkStructuredStreamingPipelineResult#cancel()}. */
@RunWith(JUnit4.class)
public class SparkStructuredStreamingPipelineResultTest {

  private static SparkStructuredStreamingPipelineResult result(
      Future<?> f, Runnable cb, ExecutorService exec) {
    return new SparkStructuredStreamingPipelineResult(
        f, () -> null, new MetricsAccumulator(), cb, exec);
  }

  private static Future<?> submit(ExecutorService exec, Runnable r) throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    Future<?> f =
        exec.submit(
            () -> {
              started.countDown();
              r.run();
            });
    exec.shutdown();
    started.await();
    return f;
  }

  @Test
  public void testCancelWaitsForExecutionThread() throws Exception {
    CountDownLatch block = new CountDownLatch(1);
    AtomicBoolean finished = new AtomicBoolean();
    AtomicBoolean observed = new AtomicBoolean();
    ExecutorService exec = Executors.newSingleThreadExecutor();
    Future<?> f =
        submit(
            exec,
            () -> {
              try {
                block.await();
              } catch (InterruptedException ignored) {
              }
              sleepUninterruptibly(300, TimeUnit.MILLISECONDS);
              finished.set(true);
            });
    SparkStructuredStreamingPipelineResult res =
        result(f, () -> observed.set(finished.get()), exec);
    assertEquals(PipelineResult.State.CANCELLED, res.cancel());
    assertTrue(observed.get());
    assertEquals(PipelineResult.State.CANCELLED, res.getState());
  }

  @Test
  public void testCancelAwaitsTaskIgnoringInterrupt() throws Exception {
    ExecutorService exec = Executors.newSingleThreadExecutor();
    Future<?> f = submit(exec, () -> sleepUninterruptibly(200, TimeUnit.MILLISECONDS));
    SparkStructuredStreamingPipelineResult res = result(f, null, exec);
    assertEquals(PipelineResult.State.CANCELLED, res.cancel());
    assertTrue(exec.isTerminated());
  }
}
