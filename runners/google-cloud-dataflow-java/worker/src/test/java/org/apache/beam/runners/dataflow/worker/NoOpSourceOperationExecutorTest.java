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

import static org.junit.Assert.assertEquals;

import com.google.api.services.dataflow.model.SourceOperationRequest;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.api.services.dataflow.model.SourceSplitRequest;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link NoOpSourceOperationExecutor} */
@RunWith(JUnit4.class)
public class NoOpSourceOperationExecutorTest {

  private PipelineOptions options;
  private NoOpSourceOperationExecutor executor;

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.fromArgs(new String[] {"--experiments=beam_fn_api"}).create();
    SourceSplitRequest splitRequest = new SourceSplitRequest();
    SourceOperationRequest operationRequest = new SourceOperationRequest().setSplit(splitRequest);
    executor = new NoOpSourceOperationExecutor(operationRequest);
  }

  @Test
  public void testNoOpSourceOperationExecutor() throws Exception {
    executor.execute();
    SourceOperationResponse response = executor.getResponse();
    assertEquals("SOURCE_SPLIT_OUTCOME_USE_CURRENT", response.getSplit().getOutcome());
  }

  @Test
  public void testNoOpSourceOperationExecutorWithNoSplitRequest() throws Exception {
    executor = new NoOpSourceOperationExecutor(new SourceOperationRequest());
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Unsupported source operation request");
    executor.execute();
  }
}
