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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

import com.google.cloud.pubsublite.Offset;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

@RunWith(JUnit4.class)
public class CheckpointMarkImplTest {

  private static final Offset OFFSET = Offset.of(42);

  @Rule public MockitoRule mockito = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Mock private BlockingCommitter committer;

  private CheckpointMarkImpl mark;

  @Before
  public void setUp() {
    mark = new CheckpointMarkImpl(OFFSET, () -> committer);
  }

  @Test
  public void testFinalize() throws Exception {
    mark.finalizeCheckpoint();
    verify(committer).commitOffset(OFFSET);
  }

  @Test
  public void encodedCheckpointFinalizeFails() throws Exception {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    CheckpointMarkImpl.coder().encode(mark, stream);
    CheckpointMarkImpl impl =
        CheckpointMarkImpl.coder().decode(new ByteArrayInputStream(stream.toByteArray()));
    assertEquals(impl.offset, OFFSET);
    // No exception thrown despite throw to work around DirectRunner issue.
    impl.finalizeCheckpoint();
  }
}
