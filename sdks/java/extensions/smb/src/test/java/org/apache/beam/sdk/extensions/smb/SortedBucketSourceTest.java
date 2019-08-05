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
package org.apache.beam.sdk.extensions.smb;

import static org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;

import java.nio.channels.Channels;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.extensions.smb.FileOperations.Writer;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link SortedBucketSource}. */
public class SortedBucketSourceTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder lhsFolder = new TemporaryFolder();
  @Rule public final TemporaryFolder rhsFolder = new TemporaryFolder();

  private SMBFilenamePolicy lhsPolicy;
  private SMBFilenamePolicy rhsPolicy;

  @Before
  public void setup() {
    lhsPolicy = new SMBFilenamePolicy(fromFolder(lhsFolder), ".txt");
    rhsPolicy = new SMBFilenamePolicy(fromFolder(rhsFolder), ".txt");
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUniformBucketsOneShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "a4", "c3", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "x4", "z3", "z4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUniformBucketsMultiShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "b1"),
            BucketShardId.of(0, 1), Lists.newArrayList("a2", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "y1"),
            BucketShardId.of(1, 1), Lists.newArrayList("x2", "y2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(0, 1), Lists.newArrayList("a4", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3"),
            BucketShardId.of(1, 1), Lists.newArrayList("x4", "z4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUniformBucketsMixedShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(0, 1), Lists.newArrayList("a4", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3"),
            BucketShardId.of(1, 1), Lists.newArrayList("x4", "z4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsOneShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2"),
            BucketShardId.of(2, 0), Lists.newArrayList("c1", "c2", "d1", "d2"),
            BucketShardId.of(3, 0), Lists.newArrayList("y1", "y2", "z1", "z2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsMultiShard() throws Exception {
    Map<BucketShardId, List<String>> lhs = new HashMap<>();
    lhs.put(BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"));
    lhs.put(BucketShardId.of(0, 1), Lists.newArrayList("a1", "a2", "b1", "b2"));
    lhs.put(BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2"));
    lhs.put(BucketShardId.of(1, 1), Lists.newArrayList("x1", "x2", "y1", "y2"));
    lhs.put(BucketShardId.of(2, 0), Lists.newArrayList("c1", "c2", "d1", "d2"));
    lhs.put(BucketShardId.of(2, 1), Lists.newArrayList("c1", "c2", "d1", "d2"));
    lhs.put(BucketShardId.of(3, 0), Lists.newArrayList("y1", "y2", "z1", "z2"));
    lhs.put(BucketShardId.of(3, 1), Lists.newArrayList("y1", "y2", "z1", "z2"));

    test(
        lhs,
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(0, 1), Lists.newArrayList("a4", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3"),
            BucketShardId.of(1, 1), Lists.newArrayList("x4", "z4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMixedBucketsMixedShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2"),
            BucketShardId.of(2, 0), Lists.newArrayList("c1", "c2", "d1", "d2"),
            BucketShardId.of(3, 0), Lists.newArrayList("y1", "y2", "z1", "z2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "c3"),
            BucketShardId.of(0, 1), Lists.newArrayList("a4", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "z3"),
            BucketShardId.of(1, 1), Lists.newArrayList("f4", "g4")));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testNullKeysIgnored() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.ofNullKey(0), Lists.newArrayList(""),
            BucketShardId.of(0, 0), Lists.newArrayList("x1", "x2", "y1", "y2"),
            BucketShardId.of(1, 0), Lists.newArrayList("c1", "c2")),
        ImmutableMap.of(
            BucketShardId.ofNullKey(0), Lists.newArrayList(""),
            BucketShardId.of(0, 0), Lists.newArrayList("x3", "x4", "z3", "z4"),
            BucketShardId.of(1, 0), Lists.newArrayList("c2", "c3")));
  }

  private void test(
      Map<BucketShardId, List<String>> lhsInput, Map<BucketShardId, List<String>> rhsInput)
      throws Exception {
    int lhsNumBuckets = maxId(lhsInput.keySet(), BucketShardId::getBucketId) + 1;
    int lhsNumShards = maxId(lhsInput.keySet(), BucketShardId::getShardId) + 1;

    int rhsNumBuckets = maxId(rhsInput.keySet(), BucketShardId::getBucketId) + 1;
    int rhsNumShards = maxId(rhsInput.keySet(), BucketShardId::getShardId) + 1;

    TestBucketMetadata lhsMetadata = TestBucketMetadata.of(lhsNumBuckets, lhsNumShards);
    TestBucketMetadata rhsMetadata = TestBucketMetadata.of(rhsNumBuckets, rhsNumShards);

    write(lhsPolicy.forDestination(), lhsMetadata, lhsInput);
    write(rhsPolicy.forDestination(), rhsMetadata, rhsInput);

    final TupleTag<String> lhsTag = new TupleTag<>("LHS");
    final TupleTag<String> rhsTag = new TupleTag<>("RHS");
    final TestFileOperations fileOperations = new TestFileOperations();
    final List<BucketedInput<?, ?>> inputs =
        Lists.newArrayList(
            new BucketedInput<>(lhsTag, fromFolder(lhsFolder), ".txt", fileOperations),
            new BucketedInput<>(rhsTag, fromFolder(rhsFolder), ".txt", fileOperations));

    PCollection<KV<String, CoGbkResult>> output =
        pipeline.apply(new SortedBucketSource<>(String.class, inputs));

    // CoGroupByKey inputs as expected result
    final Map<String, List<String>> lhs = groupByKey(lhsInput, lhsMetadata::extractKey);
    final Map<String, List<String>> rhs = groupByKey(rhsInput, rhsMetadata::extractKey);
    final Map<String, KV<List<String>, List<String>>> expected = new HashMap<>();
    for (String k : Sets.union(lhs.keySet(), rhs.keySet())) {
      List<String> l = lhs.getOrDefault(k, Collections.emptyList());
      List<String> r = rhs.getOrDefault(k, Collections.emptyList());
      expected.put(k, KV.of(l, r));
    }

    PAssert.thatMap(output)
        .satisfies(
            m -> {
              Map<String, KV<List<String>, List<String>>> actual = new HashMap<>();
              for (Map.Entry<String, CoGbkResult> kv : m.entrySet()) {
                List<String> l =
                    StreamSupport.stream(kv.getValue().getAll(lhsTag).spliterator(), false)
                        .sorted()
                        .collect(Collectors.toList());
                List<String> r =
                    StreamSupport.stream(kv.getValue().getAll(rhsTag).spliterator(), false)
                        .sorted()
                        .collect(Collectors.toList());
                actual.put(kv.getKey(), KV.of(l, r));
              }
              Assert.assertEquals(expected, actual);
              return null;
            });

    pipeline.run();
  }

  private static void write(
      FileAssignment fileAssignment,
      TestBucketMetadata metadata,
      Map<BucketShardId, List<String>> input)
      throws Exception {
    // Write bucket metadata
    BucketMetadata.to(
        metadata,
        Channels.newOutputStream(
            FileSystems.create(fileAssignment.forMetadata(), "application/json")));

    // Write bucket files
    final TestFileOperations fileOperations = new TestFileOperations();
    for (Map.Entry<BucketShardId, List<String>> entry : input.entrySet()) {
      Writer<String> writer =
          fileOperations.createWriter(fileAssignment.forBucket(entry.getKey(), metadata));
      for (String s : entry.getValue()) {
        writer.write(s);
      }
      writer.close();
    }
  }

  private static int maxId(Set<BucketShardId> ids, ToIntFunction<BucketShardId> fn) {
    return ids.stream().mapToInt(fn).max().getAsInt();
  }

  private static Map<String, List<String>> groupByKey(
      Map<BucketShardId, List<String>> input, Function<String, String> keyFn) {
    final List<String> values =
        input.values().stream().flatMap(List::stream).collect(Collectors.toList());
    return values.stream()
        .filter(v -> keyFn.apply(v) != null)
        .collect(
            Collectors.toMap(
                keyFn,
                Collections::singletonList,
                (l, r) ->
                    Stream.concat(l.stream(), r.stream()).sorted().collect(Collectors.toList())));
  }
}
