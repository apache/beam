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
package org.apache.beam.runners.dataflow.worker.util.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ResizableSemaphoreTest {

  @Test
  public void testSetTotalPermits() throws InterruptedException {
    ResizableSemaphore semaphore = new ResizableSemaphore(0);
    assertFalse(semaphore.tryAcquire());
    semaphore.setTotalPermits(1);
    assertEquals(1, semaphore.getTotalPermits());

    assertTrue(semaphore.tryAcquire());
    assertFalse(semaphore.tryAcquire());

    semaphore.release();
    semaphore.setTotalPermits(0);
    assertFalse(semaphore.tryAcquire());
    assertEquals(0, semaphore.getTotalPermits());

    semaphore.setTotalPermits(5);
    assertEquals(5, semaphore.getTotalPermits());

    semaphore.acquire(4);
    assertEquals(1, semaphore.availablePermits());
  }
}
