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
package org.apache.beam.sdk.io.cassandra;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

/** Tests on {@link SplitGenerator}. */
public final class SplitGeneratorTest {

  @Test
  public void testGenerateSegments() {
    List<BigInteger> tokens =
        Stream.of(
                "0",
                "1",
                "56713727820156410577229101238628035242",
                "56713727820156410577229101238628035243",
                "113427455640312821154458202477256070484",
                "113427455640312821154458202477256070485")
            .map(BigInteger::new)
            .collect(Collectors.toList());

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    List<List<RingRange>> segments = generator.generateSplits(10, tokens);

    assertEquals(12, segments.size());
    assertEquals("[(0,1], (1,14178431955039102644307275309657008811]]", segments.get(0).toString());
    assertEquals(
        "[(14178431955039102644307275309657008811,28356863910078205288614550619314017621]]",
        segments.get(1).toString());
    assertEquals(
        "[(70892159775195513221536376548285044053,85070591730234615865843651857942052863]]",
        segments.get(5).toString());

    tokens =
        Stream.of(
                "5",
                "6",
                "56713727820156410577229101238628035242",
                "56713727820156410577229101238628035243",
                "113427455640312821154458202477256070484",
                "113427455640312821154458202477256070485")
            .map(BigInteger::new)
            .collect(Collectors.toList());

    segments = generator.generateSplits(10, tokens);

    assertEquals(12, segments.size());
    assertEquals("[(5,6], (6,14178431955039102644307275309657008815]]", segments.get(0).toString());
    assertEquals(
        "[(70892159775195513221536376548285044053,85070591730234615865843651857942052863]]",
        segments.get(5).toString());
    assertEquals(
        "[(141784319550391026443072753096570088109,155962751505430129087380028406227096921]]",
        segments.get(10).toString());
  }

  @Test(expected = RuntimeException.class)
  public void testZeroSizeRange() {
    List<String> tokenStrings =
        Arrays.asList(
            "0",
            "1",
            "56713727820156410577229101238628035242",
            "56713727820156410577229101238628035242",
            "113427455640312821154458202477256070484",
            "113427455640312821154458202477256070485");

    List<BigInteger> tokens =
        tokenStrings.stream().map(BigInteger::new).collect(Collectors.toList());

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    generator.generateSplits(10, tokens);
  }

  @Test
  public void testRotatedRing() {
    List<String> tokenStrings =
        Arrays.asList(
            "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484",
            "113427455640312821154458202477256070485",
            "5",
            "6",
            "56713727820156410577229101238628035242");

    List<BigInteger> tokens =
        tokenStrings.stream().map(BigInteger::new).collect(Collectors.toList());

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    List<List<RingRange>> segments = generator.generateSplits(5, tokens);
    assertEquals(6, segments.size());
    assertEquals(
        "[(85070591730234615865843651857942052863,113427455640312821154458202477256070484],"
            + " (113427455640312821154458202477256070484,113427455640312821154458202477256070485]]",
        segments.get(1).toString());
    assertEquals(
        "[(113427455640312821154458202477256070485," + "141784319550391026443072753096570088109]]",
        segments.get(2).toString());
    assertEquals(
        "[(141784319550391026443072753096570088109,5], (5,6]]", segments.get(3).toString());
  }

  @Test(expected = RuntimeException.class)
  public void testDisorderedRing() {
    List<String> tokenStrings =
        Arrays.asList(
            "0",
            "113427455640312821154458202477256070485",
            "1",
            "56713727820156410577229101238628035242",
            "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484");

    List<BigInteger> tokens =
        tokenStrings.stream().map(BigInteger::new).collect(Collectors.toList());

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    generator.generateSplits(10, tokens);
    // Will throw an exception when concluding that the repair segments don't add up.
    // This is because the tokens were supplied out of order.
  }
}
