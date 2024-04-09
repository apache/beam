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
package org.apache.beam.runners.dataflow.worker.windmill.client;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GetDataClientTest {
  private GetDataClient getDataClient;

  @Before
  public void setUp() {
    MemoryMonitor memoryMonitor = mock(MemoryMonitor.class);
    getDataClient = new GetDataClient(memoryMonitor);
    doNothing().when(memoryMonitor).waitForResources(anyString());
  }

  @Test
  public void testRefreshActiveWorkWithFanOut_doesNotRefreshWhenStreamClosed() {
    WindmillStream.GetDataStream getDataStream = mock(WindmillStream.GetDataStream.class);
    when(getDataStream.isClosed()).thenReturn(true);
    getDataClient.refreshActiveWorkWithFanOut(ImmutableMap.of(getDataStream, new HashMap<>()));
    verify(getDataStream, times(0)).refreshActiveWork(any());
  }

  @Test
  public void testRefreshActiveWorkWithFanOut_refreshWorkOnError() {
    WindmillStream.GetDataStream getDataStream1 = mock(WindmillStream.GetDataStream.class);
    WindmillStream.GetDataStream getDataStream2 = mock(WindmillStream.GetDataStream.class);
    WindmillStream.GetDataStream getDataStreamError = mock(WindmillStream.GetDataStream.class);
    when(getDataStream1.isClosed()).thenReturn(false);
    when(getDataStream2.isClosed()).thenReturn(false);
    when(getDataStreamError.isClosed()).thenReturn(false);
    doNothing().when(getDataStream1).refreshActiveWork(any());
    doNothing().when(getDataStream2).refreshActiveWork(any());
    String errorMessage = "Something bad happened";
    RuntimeException refreshActiveWorkError = new RuntimeException(errorMessage);
    doThrow(refreshActiveWorkError).when(getDataStreamError).refreshActiveWork(any());

    HashMap<String, List<Windmill.HeartbeatRequest>> heartbeats1 = new HashMap<>();
    HashMap<String, List<Windmill.HeartbeatRequest>> heartbeats2 = new HashMap<>();
    HashMap<String, List<Windmill.HeartbeatRequest>> heartbeatsError = new HashMap<>();

    // Even though one of the GetDataStreams will throw, we need to make sure that a failure on one
    // GetDataStream does not stop refreshActiveWork on other GetDataStreams.
    assertThrows(
        errorMessage,
        RuntimeException.class,
        () ->
            getDataClient.refreshActiveWorkWithFanOut(
                ImmutableMap.of(
                    getDataStreamError,
                    heartbeatsError,
                    getDataStream1,
                    heartbeats1,
                    getDataStream2,
                    heartbeats2)));

    verify(getDataStream1, times(1)).refreshActiveWork(eq(heartbeats1));
    verify(getDataStream2, times(1)).refreshActiveWork(eq(heartbeats2));
    verify(getDataStreamError, times(1)).refreshActiveWork(eq(heartbeatsError));
  }

  @Test
  public void testRefreshActiveWorkWithFanOut() {
    WindmillStream.GetDataStream getDataStream1 = mock(WindmillStream.GetDataStream.class);
    WindmillStream.GetDataStream getDataStream2 = mock(WindmillStream.GetDataStream.class);
    when(getDataStream1.isClosed()).thenReturn(false);
    when(getDataStream2.isClosed()).thenReturn(false);
    doNothing().when(getDataStream1).refreshActiveWork(any());
    doNothing().when(getDataStream2).refreshActiveWork(any());

    HashMap<String, List<Windmill.HeartbeatRequest>> heartbeats1 = new HashMap<>();
    HashMap<String, List<Windmill.HeartbeatRequest>> heartbeats2 = new HashMap<>();

    getDataClient.refreshActiveWorkWithFanOut(
        ImmutableMap.of(getDataStream1, heartbeats1, getDataStream2, heartbeats2));

    verify(getDataStream1, times(1)).refreshActiveWork(eq(heartbeats1));
    verify(getDataStream2, times(1)).refreshActiveWork(eq(heartbeats2));
  }
}
