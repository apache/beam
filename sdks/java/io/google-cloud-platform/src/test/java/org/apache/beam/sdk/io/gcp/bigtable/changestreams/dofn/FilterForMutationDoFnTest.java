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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FilterForMutationDoFnTest {

  private FilterForMutationDoFn doFn;
  @Mock private DoFn.OutputReceiver<KV<ByteString, ChangeStreamMutation>> outputReceiver;

  @Before
  public void setup() {
    doFn = new FilterForMutationDoFn();
  }

  @Test
  public void shouldNotOutputHeartbeats() {
    Heartbeat heartbeat = mock(Heartbeat.class);
    doFn.processElement(KV.of(ByteString.copyFromUtf8("test"), heartbeat), outputReceiver);
    verify(outputReceiver, never()).output(any());
  }

  @Test
  public void shouldOutputChangeStreamMutations() {
    ChangeStreamMutation mutation = mock(ChangeStreamMutation.class);
    doFn.processElement(KV.of(ByteString.copyFromUtf8("test"), mutation), outputReceiver);
    verify(outputReceiver, times(1)).output(KV.of(ByteString.copyFromUtf8("test"), mutation));
  }

  @Test
  public void shouldOutputCloseStreams() {
    // This shouldn't happen but if it were to we wouldn't want the CloseStreams to be returned to
    // users
    CloseStream closeStream = mock(CloseStream.class);
    doFn.processElement(KV.of(ByteString.copyFromUtf8("test"), closeStream), outputReceiver);
    verify(outputReceiver, never()).output(any());
  }
}
