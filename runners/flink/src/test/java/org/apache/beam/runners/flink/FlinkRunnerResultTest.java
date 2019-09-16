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
package org.apache.beam.runners.flink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.Collections;
import org.apache.beam.sdk.PipelineResult;
import org.joda.time.Duration;
import org.junit.Test;

/** Tests for {@link FlinkRunnerResult}. */
public class FlinkRunnerResultTest {

  @Test
  public void testPipelineResultReturnsDone() {
    FlinkRunnerResult result = new FlinkRunnerResult(Collections.emptyMap(), 100);
    assertThat(result.getState(), is(PipelineResult.State.DONE));
  }

  @Test
  public void testWaitUntilFinishReturnsDone() {
    FlinkRunnerResult result = new FlinkRunnerResult(Collections.emptyMap(), 100);
    assertThat(result.waitUntilFinish(), is(PipelineResult.State.DONE));
    assertThat(result.waitUntilFinish(Duration.millis(100)), is(PipelineResult.State.DONE));
  }

  @Test
  public void testCancelDoesNotThrowAnException() {
    FlinkRunnerResult result = new FlinkRunnerResult(Collections.emptyMap(), 100);
    result.cancel();
    assertThat(result.getState(), is(PipelineResult.State.DONE));
  }
}
