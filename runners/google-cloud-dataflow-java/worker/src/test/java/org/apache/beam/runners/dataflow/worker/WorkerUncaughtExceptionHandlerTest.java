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
package org.apache.beam.runners.dataflow.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.beam.runners.dataflow.worker.util.common.worker.JvmRuntime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests of {@link WorkerUncaughtExceptionHandler}. */
public class WorkerUncaughtExceptionHandlerTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(WorkerUncaughtExceptionHandlerTest.class);

  @Test
  public void testUncaughtExceptionHandlerForciblyHaltsRuntime() {
    JvmRuntime runtime = mock(JvmRuntime.class);
    WorkerUncaughtExceptionHandler handler = new WorkerUncaughtExceptionHandler(runtime, LOG);

    try {
      handler.uncaughtException(Thread.currentThread(), new Exception("oh noes!"));
    } catch (Exception e) {
      // Ignore any exceptions being thrown and validate that the runtime is halted below.
    }
    verify(runtime).halt(1);
  }
}
