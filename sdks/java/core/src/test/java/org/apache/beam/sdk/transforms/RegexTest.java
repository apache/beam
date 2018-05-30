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
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Regex}. */
@RunWith(JUnit4.class)
public class RegexTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testFind() {
    PCollection<String> output =
        p.apply(Create.of("aj", "xj", "yj", "zj")).apply(Regex.find("[xyz]"));

    PAssert.that(output).containsInAnyOrder("x", "y", "z");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFindGroup() {
    PCollection<String> output =
        p.apply(Create.of("aj", "xj", "yj", "zj")).apply(Regex.find("([xyz])", 1));

    PAssert.that(output).containsInAnyOrder("x", "y", "z");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFindNone() {
    PCollection<String> output = p.apply(Create.of("a", "b", "c", "d")).apply(Regex.find("[xyz]"));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFindNameGroup() {
    PCollection<String> output =
        p.apply(Create.of("aj", "xj", "yj", "zj"))
            .apply(Regex.find("(?<namedgroup>[xyz])", "namedgroup"));

    PAssert.that(output).containsInAnyOrder("x", "y", "z");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFindAllGroups() {
    PCollection<List<String>> output =
        p.apply(Create.of("aj", "xjx", "yjy", "zjz")).apply(Regex.findAll("([xyz])j([xyz])"));

    PAssert.that(output)
        .containsInAnyOrder(
            Arrays.asList("xjx", "x", "x"),
            Arrays.asList("yjy", "y", "y"),
            Arrays.asList("zjz", "z", "z"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFindNameNone() {
    PCollection<String> output =
        p.apply(Create.of("a", "b", "c", "d"))
            .apply(Regex.find("(?<namedgroup>[xyz])", "namedgroup"));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVFind() {

    PCollection<KV<String, String>> output =
        p.apply(Create.of("a b c")).apply(Regex.findKV("a (b) (c)", 1, 2));

    PAssert.that(output).containsInAnyOrder(KV.of("b", "c"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVFindNone() {

    PCollection<KV<String, String>> output =
        p.apply(Create.of("x y z")).apply(Regex.findKV("a (b) (c)", 1, 2));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVFindName() {

    PCollection<KV<String, String>> output =
        p.apply(Create.of("a b c"))
            .apply(Regex.findKV("a (?<keyname>b) (?<valuename>c)", "keyname", "valuename"));

    PAssert.that(output).containsInAnyOrder(KV.of("b", "c"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVFindNameNone() {

    PCollection<KV<String, String>> output =
        p.apply(Create.of("x y z"))
            .apply(Regex.findKV("a (?<keyname>b) (?<valuename>c)", "keyname", "valuename"));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatches() {

    PCollection<String> output =
        p.apply(Create.of("a", "x", "y", "z")).apply(Regex.matches("[xyz]"));

    PAssert.that(output).containsInAnyOrder("x", "y", "z");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchesNone() {

    PCollection<String> output =
        p.apply(Create.of("a", "b", "c", "d")).apply(Regex.matches("[xyz]"));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchesGroup() {

    PCollection<String> output =
        p.apply(Create.of("a", "x xxx", "x yyy", "x zzz")).apply(Regex.matches("x ([xyz]*)", 1));

    PAssert.that(output).containsInAnyOrder("xxx", "yyy", "zzz");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchesName() {

    PCollection<String> output =
        p.apply(Create.of("a", "x xxx", "x yyy", "x zzz"))
            .apply(Regex.matches("x (?<namedgroup>[xyz]*)", "namedgroup"));

    PAssert.that(output).containsInAnyOrder("xxx", "yyy", "zzz");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchesNameNone() {

    PCollection<String> output =
        p.apply(Create.of("a", "b", "c", "d"))
            .apply(Regex.matches("x (?<namedgroup>[xyz]*)", "namedgroup"));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAllMatches() {

    PCollection<List<String>> output =
        p.apply(Create.of("a x", "x x", "y y", "z z")).apply(Regex.allMatches("([xyz]) ([xyz])"));

    PAssert.that(output)
        .containsInAnyOrder(
            Arrays.asList("x x", "x", "x"),
            Arrays.asList("y y", "y", "y"),
            Arrays.asList("z z", "z", "z"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVMatches() {

    PCollection<KV<String, String>> output =
        p.apply(Create.of("a b c")).apply(Regex.matchesKV("a (b) (c)", 1, 2));

    PAssert.that(output).containsInAnyOrder(KV.of("b", "c"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVMatchesNone() {

    PCollection<KV<String, String>> output =
        p.apply(Create.of("x y z")).apply(Regex.matchesKV("a (b) (c)", 1, 2));
    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVMatchesName() {

    PCollection<KV<String, String>> output =
        p.apply(Create.of("a b c"))
            .apply(Regex.findKV("a (?<keyname>b) (?<valuename>c)", "keyname", "valuename"));

    PAssert.that(output).containsInAnyOrder(KV.of("b", "c"));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKVMatchesNameNone() {

    PCollection<KV<String, String>> output =
        p.apply(Create.of("x y z"))
            .apply(Regex.findKV("a (?<keyname>b) (?<valuename>c)", "keyname", "valuename"));
    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReplaceAll() {

    PCollection<String> output =
        p.apply(Create.of("xj", "yj", "zj")).apply(Regex.replaceAll("[xyz]", "new"));

    PAssert.that(output).containsInAnyOrder("newj", "newj", "newj");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReplaceAllMixed() {

    PCollection<String> output =
        p.apply(Create.of("abc", "xj", "yj", "zj", "def")).apply(Regex.replaceAll("[xyz]", "new"));

    PAssert.that(output).containsInAnyOrder("abc", "newj", "newj", "newj", "def");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReplaceFirst() {

    PCollection<String> output =
        p.apply(Create.of("xjx", "yjy", "zjz")).apply(Regex.replaceFirst("[xyz]", "new"));

    PAssert.that(output).containsInAnyOrder("newjx", "newjy", "newjz");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReplaceFirstMixed() {

    PCollection<String> output =
        p.apply(Create.of("abc", "xjx", "yjy", "zjz", "def"))
            .apply(Regex.replaceFirst("[xyz]", "new"));

    PAssert.that(output).containsInAnyOrder("abc", "newjx", "newjy", "newjz", "def");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSplits() {

    PCollection<String> output =
        p.apply(Create.of("The  quick   brown fox jumps over    the lazy dog"))
            .apply(Regex.split("\\W+"));

    PAssert.that(output)
        .containsInAnyOrder("The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSplitsWithEmpty() {

    PCollection<String> output =
        p.apply(Create.of("The  quick   brown fox jumps over    the lazy dog"))
            .apply(Regex.split("\\s", true));

    PAssert.that(output)
        .containsInAnyOrder(
            "The", "", "quick", "brown", "", "", "fox", "jumps", "over", "", "", "", "the", "lazy",
            "dog");
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSplitsWithoutEmpty() {

    PCollection<String> output =
        p.apply(Create.of("The  quick   brown fox jumps over    the lazy dog"))
            .apply(Regex.split("\\s", false));

    PAssert.that(output)
        .containsInAnyOrder("The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog");
    p.run();
  }
}
