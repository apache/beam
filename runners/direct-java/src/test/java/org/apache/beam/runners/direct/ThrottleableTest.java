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
package org.apache.beam.runners.direct;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.local.ExecutionDriver;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the exponential back-off algorithm as provided by {@link
 * ExecutorServiceParallelExecutor.Throttleable}.
 */
public class ThrottleableTest {

  @Test
  public void testBackoff() {
    List<ExecutionDriver.DriverState> states =
        Lists.newArrayList(
            ExecutionDriver.DriverState.CONTINUE,
            ExecutionDriver.DriverState.CONTINUE_THROTTLE,
            ExecutionDriver.DriverState.CONTINUE,
            ExecutionDriver.DriverState.CONTINUE_THROTTLE,
            ExecutionDriver.DriverState.CONTINUE_THROTTLE,
            ExecutionDriver.DriverState.CONTINUE,
            ExecutionDriver.DriverState.CONTINUE_THROTTLE,
            ExecutionDriver.DriverState.CONTINUE_THROTTLE,
            ExecutionDriver.DriverState.CONTINUE_THROTTLE,
            ExecutionDriver.DriverState.CONTINUE_THROTTLE,
            ExecutionDriver.DriverState.CONTINUE_THROTTLE,
            ExecutionDriver.DriverState.CONTINUE_THROTTLE);
    List<Long> sleeps = new ArrayList<>();
    ExecutorServiceParallelExecutor.Throttleable throttleable =
        new ExecutorServiceParallelExecutor.Throttleable() {
          @Override
          public void sleep() {
            sleeps.add(delay);
          }

          @Override
          public void run() {
            throttle(states.remove(0));
          }
        };
    while (!states.isEmpty()) {
      throttleable.run();
    }
    Assert.assertEquals(
        Lists.newArrayList(
            ExecutorServiceParallelExecutor.Throttleable.DELAY_FIRST,
            ExecutorServiceParallelExecutor.Throttleable.DELAY_FIRST,
            ExecutorServiceParallelExecutor.Throttleable.DELAY_FIRST * 2,
            ExecutorServiceParallelExecutor.Throttleable.DELAY_FIRST,
            ExecutorServiceParallelExecutor.Throttleable.DELAY_FIRST * 2,
            ExecutorServiceParallelExecutor.Throttleable.DELAY_FIRST * 4,
            ExecutorServiceParallelExecutor.Throttleable.DELAY_FIRST * 8,
            ExecutorServiceParallelExecutor.Throttleable.DELAY_MAX,
            ExecutorServiceParallelExecutor.Throttleable.DELAY_MAX),
        sleeps);
  }
}
