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

import static org.apache.beam.sdk.io.synthetic.SyntheticTestUtils.optionsFromString;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SyntheticOptions}. */
@RunWith(JUnit4.class)
public class SyntheticOptionsTest {
  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testInvalidDelayDistribution() throws Exception {
    thrown.expect(JsonMappingException.class);
    String syntheticOptions = "{\"delayDistribution\":\"0\"}";
    optionsFromString(syntheticOptions, SyntheticOptions.class);
  }

  @Test
  public void testSyntheticOptionsWithNegativeKeySize() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("keySizeBytes should be a positive number, but found -64");
    String options = "{\"keySizeBytes\":-64}";
    optionsFromString(options, SyntheticOptions.class);
  }

  @Test
  public void testSyntheticOptionsWithNegativeHotKeyFraction() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("hotKeyFraction should be a non-negative number, but found -0.25");
    String options = "{\"hotKeyFraction\":-0.25}";
    optionsFromString(options, SyntheticOptions.class);
  }

  @Test
  public void testRealDistributionDeserializerWithNegativeLowerBound() throws Exception {
    thrown.expect(JsonMappingException.class);
    thrown.expectMessage(
        "lower bound of uniform distribution should be a non-negative number, but found -100.0");
    String syntheticOptions =
        "{\"delayDistribution\":{\"type\":\"uniform\",\"lower\":-100,\"upper\":200}}";
    optionsFromString(syntheticOptions, SyntheticOptions.class);
  }

  @Test
  public void testRealDistributionDeserializerWithUniformDistribution() throws Exception {
    String syntheticOptions =
        "{\"seed\":12345,"
            + "\"delayDistribution\":{\"type\":\"uniform\",\"lower\":0,\"upper\":100}}";
    SyntheticOptions sourceOptions = optionsFromString(syntheticOptions, SyntheticOptions.class);
    assertEquals(
        0,
        (long)
            ((UniformRealDistribution) sourceOptions.delayDistribution.getDistribution())
                .getSupportLowerBound());
    assertEquals(
        100,
        (long)
            ((UniformRealDistribution) sourceOptions.delayDistribution.getDistribution())
                .getSupportUpperBound());
  }

  @Test
  public void testRealDistributionDeserializerWithNormalDistribution() throws Exception {
    String syntheticOptions =
        "{\"seed\":12345,"
            + "\"delayDistribution\":{\"type\":\"normal\",\"mean\":100,\"stddev\":50}}";
    SyntheticOptions sourceOptions = optionsFromString(syntheticOptions, SyntheticOptions.class);
    assertEquals(
        100,
        (long) ((NormalDistribution) sourceOptions.delayDistribution.getDistribution()).getMean());
    assertEquals(
        50,
        (long)
            ((NormalDistribution) sourceOptions.delayDistribution.getDistribution())
                .getStandardDeviation());
  }

  @Test
  public void testRealDistributionDeserializerWithExpDistribution() throws Exception {
    String syntheticOptions =
        "{\"seed\":12345," + "\"delayDistribution\":{\"type\":\"exp\",\"mean\":10}}";
    SyntheticOptions sourceOptions = optionsFromString(syntheticOptions, SyntheticOptions.class);
    assertEquals(
        10,
        (long)
            ((ExponentialDistribution) sourceOptions.delayDistribution.getDistribution())
                .getMean());
  }

  @Test
  public void testValidateHotKeys() {
    SyntheticOptions options = new SyntheticOptions();
    // keySizeBytes too low to represent a hot key.
    options.keySizeBytes = 1;
    options.hotKeyFraction = 0.1;
    options.numHotKeys = 123;
    options.setSeed(12345);
    thrown.expect(IllegalArgumentException.class);
    options.validate();
  }

  @Test
  public void testGenerateKvPairWithHotKey() {
    SyntheticOptions options = new SyntheticOptions();
    // keySizeBytes big enough to represent a hot key.
    options.keySizeBytes = 4;
    // Force generating a hot key.
    options.hotKeyFraction = 1.0;
    options.numHotKeys = 123;
    options.setSeed(12345);
    options.genKvPair(34567);
  }

  @Test
  public void testGenerateKvPairWithRegularKey() {
    SyntheticOptions options = new SyntheticOptions();
    options.keySizeBytes = 3;
    options.hotKeyFraction = 0.0;
    options.numHotKeys = 123;
    options.setSeed(12345);
    options.genKvPair(34567);
  }
}
