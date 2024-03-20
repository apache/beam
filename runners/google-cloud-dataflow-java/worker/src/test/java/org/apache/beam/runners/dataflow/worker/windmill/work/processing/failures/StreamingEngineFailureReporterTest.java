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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.Status;
import com.google.common.truth.Correspondence;
import com.google.rpc.Code;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingEngineFailureReporterTest {

  private static final String DEFAULT_COMPUTATION_ID = "computationId";

  private static FailureReporter streamingEngineFailureReporter() {
    return StreamingEngineFailureReporter.create(10, 10);
  }

  private static Windmill.WorkItem workItem() {
    return Windmill.WorkItem.newBuilder()
        .setKey(ByteString.EMPTY)
        .setWorkToken(1L)
        .setCacheToken(1L)
        .setShardingKey(1L)
        .build();
  }

  @Test
  public void testReportFailure_returnsTrue() {
    FailureReporter failureReporter = streamingEngineFailureReporter();
    assertTrue(
        failureReporter.reportFailure(DEFAULT_COMPUTATION_ID, workItem(), new RuntimeException()));
  }

  @Test
  public void testReportFailure_addsPendingErrors() {
    FailureReporter failureReporter = streamingEngineFailureReporter();
    failureReporter.reportFailure(DEFAULT_COMPUTATION_ID, workItem(), new RuntimeException());
    failureReporter.reportFailure(DEFAULT_COMPUTATION_ID, workItem(), new RuntimeException());
    failureReporter.reportFailure(DEFAULT_COMPUTATION_ID, workItem(), new RuntimeException());

    assertThat(failureReporter.get()).hasSize(3);
  }

  @Test
  public void testGet_correctlyCreatesErrorStatus() {
    FailureReporter failureReporter = streamingEngineFailureReporter();
    RuntimeException error = new RuntimeException();
    failureReporter.reportFailure(DEFAULT_COMPUTATION_ID, workItem(), error);
    assertThat(failureReporter.get())
        .comparingElementsUsing(
            Correspondence.from(
                (Status a, Status b) ->
                    a.getCode().equals(b.getCode()) && a.getMessage().contains(b.getMessage()),
                "Assert that both status codes are the same, and b contains a message."))
        .containsExactly(
            new Status().setCode(Code.UNKNOWN.getNumber()).setMessage(error.toString()));
  }

  @Test
  public void testGet_clearsPendingErrors() {
    FailureReporter failureReporter = streamingEngineFailureReporter();
    failureReporter.reportFailure(DEFAULT_COMPUTATION_ID, workItem(), new RuntimeException());
    failureReporter.reportFailure(DEFAULT_COMPUTATION_ID, workItem(), new RuntimeException());

    failureReporter.get();
    assertThat(failureReporter.get()).isEmpty();
  }
}
