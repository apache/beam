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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link RegexTransform}.
 */
@RunWith(JUnit4.class)
public class RegexTransformTest implements Serializable {
  @Test
  @Category(NeedsRunner.class)
  public void testFind() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("aj", "xj", "yj", "zj"))
        .apply(RegexTransform.find("[xyz]"));

    PAssert.that(output).containsInAnyOrder("x", "y", "z");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFindGroup() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("aj", "xj", "yj", "zj"))
        .apply(RegexTransform.find("([xyz])", 1));

    PAssert.that(output).containsInAnyOrder("x", "y", "z");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFindNone() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("a", "b", "c", "d"))
        .apply(RegexTransform.find("[xyz]"));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVFind() {
    TestPipeline p = TestPipeline.create();

    PCollection<KV<String, String>> output = p
        .apply(Create.of("a b c"))
        .apply(RegexTransform.findKV("a (b) (c)", 1, 2));

    PAssert.that(output).containsInAnyOrder(KV.of("b", "c"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVFindNone() {
    TestPipeline p = TestPipeline.create();

    PCollection<KV<String, String>> output = p
        .apply(Create.of("x y z"))
        .apply(RegexTransform.findKV("a (b) (c)", 1, 2));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatches() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("a", "x", "y", "z"))
        .apply(RegexTransform.matches("[xyz]"));

    PAssert.that(output).containsInAnyOrder("x", "y", "z");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchesNone() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("a", "b", "c", "d"))
        .apply(RegexTransform.matches("[xyz]"));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchesGroup() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("a", "x xxx", "x yyy", "x zzz"))
        .apply(RegexTransform.matches("x ([xyz]*)", 1));

    PAssert.that(output).containsInAnyOrder("xxx", "yyy", "zzz");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVMatches() {
    TestPipeline p = TestPipeline.create();

    PCollection<KV<String, String>> output = p
        .apply(Create.of("a b c"))
        .apply(RegexTransform.matchesKV("a (b) (c)", 1, 2));

    PAssert.that(output).containsInAnyOrder(KV.of("b", "c"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVMatchesNone() {
    TestPipeline p = TestPipeline.create();

    PCollection<KV<String, String>> output = p
        .apply(Create.of("x y z"))
        .apply(RegexTransform.matchesKV("a (b) (c)", 1, 2));
    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReplaceAll() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("xj", "yj", "zj"))
        .apply(RegexTransform.replaceAll("[xyz]", "new"));

    PAssert.that(output).containsInAnyOrder("newj", "newj", "newj");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReplaceAllMixed() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("abc", "xj", "yj", "zj", "def"))
        .apply(RegexTransform.replaceAll("[xyz]", "new"));

    PAssert.that(output).containsInAnyOrder("abc", "newj", "newj", "newj", "def");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReplaceFirst() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("xjx", "yjy", "zjz"))
        .apply(RegexTransform.replaceFirst("[xyz]", "new"));

    PAssert.that(output).containsInAnyOrder("newjx", "newjy", "newjz");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReplaceFirstMixed() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("abc", "xjx", "yjy", "zjz", "def"))
        .apply(RegexTransform.replaceFirst("[xyz]", "new"));

    PAssert.that(output).containsInAnyOrder("abc", "newjx", "newjy", "newjz", "def");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSplits() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("The  quick   brown fox jumps over    the lazy dog"))
        .apply(RegexTransform.split("\\W+"));

    PAssert.that(output).containsInAnyOrder("The", "quick", "brown",
      "fox", "jumps", "over", "the", "lazy", "dog");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSplitsWithEmpty() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("The  quick   brown fox jumps over    the lazy dog"))
        .apply(RegexTransform.split("\\s", true));

    String[] outputStr = "The  quick   brown fox jumps over    the lazy dog".split("\\s");

    PAssert.that(output).containsInAnyOrder("The", "", "quick", "brown", "", "",
      "fox", "jumps", "over", "", "", "", "the", "lazy", "dog");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSplitsWithoutEmpty() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.of("The  quick   brown fox jumps over    the lazy dog"))
        .apply(RegexTransform.split("\\s", false));

    PAssert.that(output).containsInAnyOrder("The", "quick", "brown",
      "fox", "jumps", "over", "the", "lazy", "dog");
    p.run();
  }
}
