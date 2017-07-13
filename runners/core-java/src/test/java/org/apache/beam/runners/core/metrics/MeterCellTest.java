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

package org.apache.beam.runners.core.metrics;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.metrics.MetricName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CounterCell}.
 */
@RunWith(JUnit4.class)
public class MeterCellTest {

  private MeterCell cell = new MeterCell(MetricName.named("hello", "world"));

  @Test
  public void testDeltaAndCumulative() {
    cell.mark(5);
    cell.mark(7);
    assertThat(cell.getCumulative().count(), equalTo(12L));
    assertThat("getCumulative is idempotent", cell.getCumulative().count(), equalTo(12L));

    assertThat(cell.getDirty().beforeCommit(), equalTo(true));
    cell.getDirty().afterCommit();
    assertThat(cell.getDirty().beforeCommit(), equalTo(false));
    assertThat(cell.getCumulative().count(), equalTo(12L));

    cell.mark(30);
    assertThat(cell.getCumulative().count(), equalTo(42L));

    assertThat(cell.getDirty().beforeCommit(), equalTo(true));
    cell.getDirty().afterCommit();
    assertThat(cell.getDirty().beforeCommit(), equalTo(false));
  }
}
