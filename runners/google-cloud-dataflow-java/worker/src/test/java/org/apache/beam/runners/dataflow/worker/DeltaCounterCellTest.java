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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.metrics.MetricName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DeltaCounterCell}. */
@RunWith(JUnit4.class)
public class DeltaCounterCellTest {

  private DeltaCounterCell cell;

  @Before
  public void setUp() {
    cell = new DeltaCounterCell(MetricName.named("hello", "world"));
  }

  @Test
  public void testDelta() {
    cell.inc(5);
    cell.inc(7);
    assertThat(cell.getSumAndReset(), equalTo(12L));
    assertThat(cell.getSumAndReset(), equalTo(0L));

    cell.inc(5);
    assertThat(cell.getSumAndReset(), equalTo(5L));
  }

  /**
   * Ensure that incrementing and getSumAndRest are correct under concurrent reader and writer
   * threads.
   */
  @Test(timeout = 30 * 1000)
  public void testAtomicSumAndReset()
      throws InterruptedException, ExecutionException, TimeoutException {
    long updatesPerWriter = 10_000;
    int numReaders = 3;
    int numWriters = 5;
    ExecutorService executor = Executors.newFixedThreadPool(numReaders + numWriters);
    AtomicBoolean doneWriting = new AtomicBoolean(false);

    Callable<Long> reader =
        () -> {
          long count = 0;
          boolean isLastRead;
          do {
            isLastRead = doneWriting.get();
            count += cell.getSumAndReset();
          } while (!isLastRead);
          return count;
        };
    Runnable writer =
        () -> {
          for (int i = 0; i < updatesPerWriter; i++) {
            cell.inc();
          }
        };

    // NB: Readers are invoked before writers to ensure they execute
    // concurrently.
    List<Future<Long>> results = new ArrayList<>(numReaders);
    for (int i = 0; i < numReaders; i++) {
      results.add(executor.submit(reader));
    }

    executor.invokeAll(Collections.nCopies(numWriters, Executors.callable(writer, 0L)));
    doneWriting.set(true);

    long count = 0;
    for (Future<Long> result : results) {
      count += result.get();
    }
    assertThat(count, equalTo(numWriters * updatesPerWriter));
  }
}
