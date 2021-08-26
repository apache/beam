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

import static org.apache.beam.sdk.io.synthetic.SyntheticOptions.fromIntegerDistribution;
import static org.apache.beam.sdk.io.synthetic.SyntheticOptions.fromRealDistribution;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions.ProgressShape;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.math3.distribution.ConstantRealDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SyntheticBoundedSource}. */
@RunWith(JUnit4.class)
public class SyntheticBoundedSourceTest {
  @Rule public final ExpectedException thrown = ExpectedException.none();

  private SyntheticSourceOptions testSourceOptions = new SyntheticSourceOptions();

  @Before
  public void setUp() {
    testSourceOptions.splitPointFrequencyRecords = 1;
    testSourceOptions.numRecords = 10;
    testSourceOptions.keySizeBytes = 10;
    testSourceOptions.valueSizeBytes = 20;
    testSourceOptions.numHotKeys = 3;
    testSourceOptions.hotKeyFraction = 0.3;
    testSourceOptions.setSeed(123456);
    testSourceOptions.bundleSizeDistribution =
        fromIntegerDistribution(new ZipfDistribution(100, 2.5));
    testSourceOptions.forceNumInitialBundles = null;
  }

  private SyntheticSourceOptions fromString(String jsonString) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    SyntheticSourceOptions result = mapper.readValue(jsonString, SyntheticSourceOptions.class);
    result.validate();
    return result;
  }

  @Test
  public void testInvalidSourceOptionsJsonFormat() throws Exception {
    thrown.expect(JsonParseException.class);
    String syntheticSourceOptions = "input:unknown URI";
    fromString(syntheticSourceOptions);
  }

  @Test
  public void testFromString() throws Exception {
    String syntheticSourceOptions =
        "{\"numRecords\":100,\"splitPointFrequencyRecords\":10,\"keySizeBytes\":10,"
            + "\"valueSizeBytes\":20,\"numHotKeys\":3,"
            + "\"hotKeyFraction\":0.3,\"seed\":123456,"
            + "\"bundleSizeDistribution\":{\"type\":\"const\",\"const\":42},"
            + "\"forceNumInitialBundles\":10,\"progressShape\":\"LINEAR_REGRESSING\""
            + "}";
    SyntheticSourceOptions sourceOptions = fromString(syntheticSourceOptions);
    assertEquals(100, sourceOptions.numRecords);
    assertEquals(10, sourceOptions.splitPointFrequencyRecords);
    assertEquals(10, sourceOptions.keySizeBytes);
    assertEquals(20, sourceOptions.valueSizeBytes);
    assertEquals(3, sourceOptions.numHotKeys);
    assertEquals(0.3, sourceOptions.hotKeyFraction, 0);
    assertEquals(0, sourceOptions.nextDelay(sourceOptions.seed));
    assertEquals(123456, sourceOptions.seed);
    assertEquals(42, sourceOptions.bundleSizeDistribution.sample(123), 0.0);
    assertEquals(10, sourceOptions.forceNumInitialBundles.intValue());
    assertEquals(ProgressShape.LINEAR_REGRESSING, sourceOptions.progressShape);
  }

  @Test
  public void testSourceOptionsWithNegativeNumRecords() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("numRecords should be a non-negative number, but found -100");
    testSourceOptions.numRecords = -100;
    testSourceOptions.validate();
  }

  /** Test the reader and the source produces the same records. */
  @Test
  public void testSourceAndReadersWork() throws Exception {
    testSourceAndReadersWorkP(1);
    testSourceAndReadersWorkP(-1);
    testSourceAndReadersWorkP(3);
  }

  private void testSourceAndReadersWorkP(long splitPointFrequency) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    testSourceOptions.splitPointFrequencyRecords = splitPointFrequency;
    SyntheticBoundedSource source = new SyntheticBoundedSource(testSourceOptions);
    assertEquals(10 * (10 + 20), source.getEstimatedSizeBytes(options));
    SourceTestUtils.assertUnstartedReaderReadsSameAsItsSource(
        source.createReader(options), options);
  }

  @Test
  public void testSplitAtFraction() throws Exception {
    testSplitAtFractionP(1);
    testSplitAtFractionP(3);
    // Do not test "-1" because then splits would be vacuous
  }

  private void testSplitAtFractionP(long splitPointFrequency) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    testSourceOptions.splitPointFrequencyRecords = splitPointFrequency;
    SyntheticBoundedSource source = new SyntheticBoundedSource(testSourceOptions);
    SourceTestUtils.assertSplitAtFractionExhaustive(source, options);
    // Can't split if already consumed.
    SourceTestUtils.assertSplitAtFractionFails(source, 5, 0.3, options);
    SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(source, 1, 0.3, options);
  }

  @Test
  public void testSplitIntoBundles() throws Exception {
    testSplitIntoBundlesP(1);
    testSplitIntoBundlesP(-1);
    testSplitIntoBundlesP(5);

    PipelineOptions options = PipelineOptionsFactory.create();
    testSourceOptions.forceNumInitialBundles = 37;
    assertEquals(37, new SyntheticBoundedSource(testSourceOptions).split(42, options).size());
  }

  private void testSplitIntoBundlesP(long splitPointFrequency) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    testSourceOptions.splitPointFrequencyRecords = splitPointFrequency;
    testSourceOptions.numRecords = 100;
    SyntheticBoundedSource source = new SyntheticBoundedSource(testSourceOptions);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(10, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(40, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(100, options), options);
  }

  @Test
  public void testIncreasingProgress() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    testSourceOptions.progressShape = ProgressShape.LINEAR;
    SyntheticBoundedSource source = new SyntheticBoundedSource(testSourceOptions);
    BoundedSource.BoundedReader<KV<byte[], byte[]>> reader = source.createReader(options);
    // Reader starts at 0.0 progress.
    assertEquals(0, reader.getFractionConsumed(), 1e-5);
    // Set the lastFractionConsumed < 0.0 so that we can use strict inequality in the below loop.
    double lastFractionConsumed = -1.0;
    for (boolean more = reader.start(); more; more = reader.advance()) {
      assertTrue(reader.getFractionConsumed() > lastFractionConsumed);
      lastFractionConsumed = reader.getFractionConsumed();
    }
    assertEquals(1, reader.getFractionConsumed(), 1e-5);
  }

  @Test
  public void testRegressingProgress() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    testSourceOptions.progressShape = ProgressShape.LINEAR_REGRESSING;
    SyntheticBoundedSource source = new SyntheticBoundedSource(testSourceOptions);
    BoundedSource.BoundedReader<KV<byte[], byte[]>> reader = source.createReader(options);
    double lastFractionConsumed = reader.getFractionConsumed();
    for (boolean more = reader.start(); more; more = reader.advance()) {
      assertTrue(reader.getFractionConsumed() <= lastFractionConsumed);
      lastFractionConsumed = reader.getFractionConsumed();
    }
  }

  @Test
  public void testSplitIntoSingleRecordBundles() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    SyntheticSourceOptions sourceOptions = new SyntheticSourceOptions();
    sourceOptions.numRecords = 10;
    sourceOptions.setSeed(123456);
    sourceOptions.bundleSizeDistribution = fromRealDistribution(new ConstantRealDistribution(1.0));
    sourceOptions.forceNumInitialBundles = 10;
    SyntheticBoundedSource source = new SyntheticBoundedSource(sourceOptions);
    List<SyntheticBoundedSource> sources = source.split(42L, options);
    for (SyntheticBoundedSource recordSource : sources) {
      recordSource.validate();
      assertEquals(1, recordSource.getEndOffset() - recordSource.getStartOffset());
    }
    SourceTestUtils.assertSourcesEqualReferenceSource(source, sources, options);
  }
}
