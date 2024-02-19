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
package org.apache.beam.sdk.util.construction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ConnectivityState;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannelBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

public class BaseExternalTest {
  @Rule public transient TestPipeline testPipeline = TestPipeline.create();
  protected static String expansionAddr;

  @BeforeClass
  public static void setUpClass() {
    expansionAddr =
        String.format("localhost:%s", Integer.valueOf(System.getProperty("expansionPort")));
  }

  @Before
  public void setUp() {
    waitForReady();
  }

  @After
  public void tearDown() {
    PipelineResult pipelineResult = testPipeline.run();
    pipelineResult.waitUntilFinish();
    assertThat(pipelineResult.getState(), equalTo(PipelineResult.State.DONE));
  }

  private void waitForReady() {
    try {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(expansionAddr).build();
      ConnectivityState state = channel.getState(true);
      for (int retry = 0; retry < 30 && state != ConnectivityState.READY; retry++) {
        Thread.sleep(500);
        state = channel.getState(true);
      }
      channel.shutdownNow();
    } catch (InterruptedException e) {
      throw new RuntimeException("interrupted.");
    }
  }
}
