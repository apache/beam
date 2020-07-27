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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class OffsetCheckpointMarkTest {
  @Captor private ArgumentCaptor<Map<Partition, Offset>> mapCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void finalizeFinalizesWithOffsets() throws Exception {
    Map<Partition, Offset> map =
        ImmutableMap.of(Partition.of(10), Offset.of(15), Partition.of(85), Offset.of(0));
    OffsetFinalizer finalizer = mock(OffsetFinalizer.class);
    OffsetCheckpointMark mark = new OffsetCheckpointMark(finalizer, map);
    mark.finalizeCheckpoint();
    verify(finalizer).finalizeOffsets(mapCaptor.capture());
    assertThat(mapCaptor.getValue(), equalTo(map));
  }

  @Test
  public void coderDropsFinalizerKeepsOffsets() throws Exception {
    Coder<OffsetCheckpointMark> coder = OffsetCheckpointMark.getCoder();
    OffsetFinalizer finalizer = mock(OffsetFinalizer.class);
    OffsetCheckpointMark mark =
        new OffsetCheckpointMark(
            finalizer,
            ImmutableMap.of(Partition.of(10), Offset.of(15), Partition.of(85), Offset.of(0)));

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(mark, output);
    ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
    OffsetCheckpointMark decoded = coder.decode(input);
    assertThat(mark.partitionOffsetMap, equalTo(decoded.partitionOffsetMap));
    decoded.finalizeCheckpoint();
    verifyZeroInteractions(finalizer);
  }
}
