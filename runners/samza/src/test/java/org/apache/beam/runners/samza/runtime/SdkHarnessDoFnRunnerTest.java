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
package org.apache.beam.runners.samza.runtime;

import java.util.concurrent.TimeoutException;
import org.junit.Test;

public class SdkHarnessDoFnRunnerTest {

  @Test(expected = TimeoutException.class)
  public void testRunWithTimeoutOccurred() throws Exception {
    SamzaDoFnRunners.SdkHarnessDoFnRunner.runWithTimeout(
        100,
        () -> {
          try {
            Thread.sleep(500);
          } catch (InterruptedException ignored) {
          }
        });
  }

  @Test
  public void testRunWithTimeoutDisabled() throws Exception {
    SamzaDoFnRunners.SdkHarnessDoFnRunner.runWithTimeout(
        -1,
        () -> {
          try {
            Thread.sleep(500);
          } catch (InterruptedException ignored) {
          }
        });
  }
}
