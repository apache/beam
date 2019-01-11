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

package org.apache.beam.sdk.runners.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test the TestCountingSource.
 */
@RunWith(JUnit4.class)
public class TestCountingSourceTest {
  @Test
  public void testRespectsCheckpointContract() throws IOException {
    TestCountingSource source = new TestCountingSource(3);
    PipelineOptions options = PipelineOptionsFactory.create();
    TestCountingSource.CountingSourceReader reader =
        source.createReader(options, null /* no checkpoint */);
    assertTrue(reader.start());
    assertEquals(0L, (long) reader.getCurrent().getValue());
    assertTrue(reader.advance());
    assertEquals(1L, (long) reader.getCurrent().getValue());
    TestCountingSource.CounterMark checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader = source.createReader(options, checkpoint);
    assertTrue(reader.start());
    assertEquals(2L, (long) reader.getCurrent().getValue());
    assertFalse(reader.advance());
  }

  @Test
  public void testCanResumeWithExpandedCount() throws IOException {
    TestCountingSource source = new TestCountingSource(1);
    PipelineOptions options = PipelineOptionsFactory.create();
    TestCountingSource.CountingSourceReader reader =
        source.createReader(options, null /* no checkpoint */);
    assertTrue(reader.start());
    assertEquals(0L, (long) reader.getCurrent().getValue());
    assertFalse(reader.advance());
    TestCountingSource.CounterMark checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    source = new TestCountingSource(2);
    reader = source.createReader(options, checkpoint);
    assertTrue(reader.start());
    assertEquals(1L, (long) reader.getCurrent().getValue());
    assertFalse(reader.advance());
  }
}
