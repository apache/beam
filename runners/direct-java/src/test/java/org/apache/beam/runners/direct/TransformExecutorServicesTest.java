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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.MoreExecutors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.ExecutorService;

/**
 * Tests for {@link TransformExecutorServices}.
 */
@RunWith(JUnit4.class)
public class TransformExecutorServicesTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private ExecutorService executorService;

  @Before
  public void setup() {
    executorService = MoreExecutors.newDirectExecutorService();
  }

  @Test
  public void parallelScheduleMultipleSchedulesBothImmediately() {
    @SuppressWarnings("unchecked")
    TransformExecutor<Object> first = mock(TransformExecutor.class);
    @SuppressWarnings("unchecked")
    TransformExecutor<Object> second = mock(TransformExecutor.class);

    TransformExecutorService parallel =
        TransformExecutorServices.parallel(executorService);
    parallel.schedule(first);
    parallel.schedule(second);

    verify(first).run();
    verify(second).run();

    parallel.complete(first);
    parallel.complete(second);
  }

  @Test
  public void serialScheduleTwoWaitsForFirstToComplete() {
    @SuppressWarnings("unchecked")
    TransformExecutor<Object> first = mock(TransformExecutor.class);
    @SuppressWarnings("unchecked")
    TransformExecutor<Object> second = mock(TransformExecutor.class);

    TransformExecutorService serial = TransformExecutorServices.serial(executorService);
    serial.schedule(first);
    verify(first).run();

    serial.schedule(second);
    verify(second, never()).run();

    serial.complete(first);
    verify(second).run();

    serial.complete(second);
  }

  @Test
  public void serialCompleteNotExecutingTaskThrows() {
    @SuppressWarnings("unchecked")
    TransformExecutor<Object> first = mock(TransformExecutor.class);
    @SuppressWarnings("unchecked")
    TransformExecutor<Object> second = mock(TransformExecutor.class);

    TransformExecutorService serial = TransformExecutorServices.serial(executorService);
    serial.schedule(first);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("unexpected currently executing");

    serial.complete(second);
  }
}
