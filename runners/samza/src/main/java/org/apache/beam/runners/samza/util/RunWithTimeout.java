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
package org.apache.beam.runners.samza.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RunWithTimeout {

  /**
   * Run a function and wait for at most the given time (in milliseconds).
   *
   * @param timeoutInMs the time to wait for completing the function call. If the value of timeout
   *     is negative, wait forever until the function call is completed
   * @param runnable the main function
   */
  public static void run(long timeoutInMs, Runnable runnable)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (timeoutInMs < 0) {
      runnable.run();
    } else {
      CompletableFuture.runAsync(runnable).get(timeoutInMs, TimeUnit.MILLISECONDS);
    }
  }
}
