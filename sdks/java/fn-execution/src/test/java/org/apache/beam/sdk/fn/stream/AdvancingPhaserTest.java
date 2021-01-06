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
package org.apache.beam.sdk.fn.stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertFalse;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link AdvancingPhaser}. */
@RunWith(JUnit4.class)
public class AdvancingPhaserTest {
  @Test
  public void testAdvancement() throws Exception {
    final AdvancingPhaser phaser = new AdvancingPhaser(1);
    int currentPhase = phaser.getPhase();
    ExecutorService service = Executors.newSingleThreadExecutor();
    service.submit((Runnable) phaser::arrive).get();
    phaser.awaitAdvance(currentPhase);
    assertFalse(phaser.isTerminated());
    service.shutdown();
    if (!service.awaitTermination(10, TimeUnit.SECONDS)) {
      assertThat(service.shutdownNow(), empty());
    }
  }
}
