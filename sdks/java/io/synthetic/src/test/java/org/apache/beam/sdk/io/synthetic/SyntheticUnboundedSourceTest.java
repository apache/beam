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
package org.apache.beam.sdk.io.synthetic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SyntheticUnboundedSource}. */
@RunWith(JUnit4.class)
public class SyntheticUnboundedSourceTest {

  private SyntheticSourceOptions sourceOptions;

  private SyntheticUnboundedSource source;

  private SyntheticRecordsCheckpoint checkpoint;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() throws IOException {
    String optionsJson =
        "{\"numRecords\":100,\"splitPointFrequencyRecords\":10,\"keySizeBytes\":10,"
            + "\"valueSizeBytes\":20,\"numHotKeys\":3,"
            + "\"hotKeyFraction\":0.3,\"seed\":123456,"
            + "\"bundleSizeDistribution\":{\"type\":\"const\",\"const\":42},"
            + "\"forceNumInitialBundles\":10}";

    sourceOptions = SyntheticTestUtils.optionsFromString(optionsJson, SyntheticSourceOptions.class);

    source = new SyntheticUnboundedSource(sourceOptions);
    checkpoint = new SyntheticRecordsCheckpoint(0);
  }

  @Test
  public void checkpointCoderShouldBeSane() {
    CoderProperties.coderSerializable(source.getCheckpointMarkCoder());
  }

  @Test
  public void coderShouldBeSane() {
    CoderProperties.coderSerializable(source.getOutputCoder());
  }

  @Test
  public void shouldStartTheReaderSuccessfully() throws IOException {
    UnboundedSource.UnboundedReader<KV<byte[], byte[]>> reader =
        source.createReader(pipeline.getOptions(), checkpoint);

    boolean isStarted = reader.start();
    assertTrue(isStarted);
  }

  @Test
  public void startPositionShouldBeExclusive() throws IOException {
    int startPosition = 0;
    checkpoint = new SyntheticRecordsCheckpoint(startPosition);

    UnboundedSource.UnboundedReader<KV<byte[], byte[]>> reader =
        source.createReader(pipeline.getOptions(), checkpoint);

    reader.start();
    KV<byte[], byte[]> currentElement = reader.getCurrent();
    KV<byte[], byte[]> expectedElement = sourceOptions.genRecord(startPosition + 1).kv;

    assertEquals(expectedElement, currentElement);
  }

  @Test
  public void lastElementShouldBeInclusive() throws IOException {
    int endPosition = 2;
    checkpoint = new SyntheticRecordsCheckpoint(0);

    UnboundedSource.UnboundedReader<KV<byte[], byte[]>> reader =
        source.createReader(pipeline.getOptions(), checkpoint);

    reader.start();
    reader.advance();
    KV<byte[], byte[]> currentElement = reader.getCurrent();
    KV<byte[], byte[]> expectedElement = sourceOptions.genRecord(endPosition).kv;

    assertEquals(expectedElement, currentElement);
  }

  @Test
  public void shouldCreateSplitsOfCountProvidedInOptions() throws IOException {
    sourceOptions.forceNumInitialBundles = 20;

    source = new SyntheticUnboundedSource(sourceOptions);

    // desiredNumSplits should be ignored if we specify forceNumInitialBundles
    Integer splitCount = source.split(100000, pipeline.getOptions()).size();

    assertEquals(sourceOptions.forceNumInitialBundles, splitCount);
  }
}
