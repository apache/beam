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
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.FileOperations.Writer;
import org.apache.beam.sdk.extensions.smb.SMBCoGbkResult.ToFinalResult;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
  public void testOneShard() throws Exception {
    test(
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a1", "a2", "b1", "b2"),
            BucketShardId.of(1, 0), Lists.newArrayList("x1", "x2", "y1", "y2")),
        ImmutableMap.of(
            BucketShardId.of(0, 0), Lists.newArrayList("a3", "a4", "c3", "c4"),
            BucketShardId.of(1, 0), Lists.newArrayList("x3", "x4", "z3", "z4")));
  }

  @Test
  public void testMultiShard() throws Exception {
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
  public void testMixedShard() throws Exception {
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

    PCollection<KV<String, KV<List<String>, List<String>>>> output =
        pipeline.apply(new SortedBucketSource<>(inputs, String.class, new ToLists(lhsTag, rhsTag)));

    // CoGroup by key inputs as expected result
    final Map<String, List<String>> lhs = groupByKey(lhsInput);
    final Map<String, List<String>> rhs = groupByKey(rhsInput);
    final Map<String, KV<List<String>, List<String>>> expected = new HashMap<>();
    for (String k : Sets.union(lhs.keySet(), rhs.keySet())) {
      List<String> l = lhs.getOrDefault(k, Collections.emptyList());
      List<String> r = rhs.getOrDefault(k, Collections.emptyList());
      expected.put(k, KV.of(l, r));
    }

    PAssert.thatMap(output)
        .satisfies(
            m -> {
              Assert.assertEquals(expected, m);
              return null;
            });

    pipeline.run();
  }

  private static class ToLists extends ToFinalResult<KV<List<String>, List<String>>> {
    final TupleTag<String> lhsTag;
    final TupleTag<String> rhsTag;

    private ToLists(TupleTag<String> lhsTag, TupleTag<String> rhsTag) {
      this.lhsTag = lhsTag;
      this.rhsTag = rhsTag;
    }

    @Override
    public KV<List<String>, List<String>> apply(SMBCoGbkResult input) {
      // Sort values for easy equality check
      List<String> lhs = Lists.newArrayList(input.getAll(lhsTag));
      List<String> rhs = Lists.newArrayList(input.getAll(rhsTag));
      lhs.sort(String::compareTo);
      rhs.sort(String::compareTo);
      return KV.of(lhs, rhs);
    }

    @Override
    public Coder<KV<List<String>, List<String>>> resultCoder() {
      return KvCoder.of(ListCoder.of(StringUtf8Coder.of()), ListCoder.of(StringUtf8Coder.of()));
    }
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
    for (Map.Entry<BucketShardId, List<String>> entry : input.entrySet()) {
      Writer<String> writer = new TestFileOperations().createWriter();
      writer.prepareWrite(
          FileSystems.create(
              fileAssignment.forBucket(entry.getKey(), metadata), writer.getMimeType()));
      for (String s : entry.getValue()) {
        writer.write(s);
      }
      writer.finishWrite();
    }
  }

  private static int maxId(Set<BucketShardId> ids, ToIntFunction<BucketShardId> fn) {
    return ids.stream().mapToInt(fn).max().getAsInt();
  }

  private static Map<String, List<String>> groupByKey(Map<BucketShardId, List<String>> input) {
    final List<String> values =
        input.values().stream().flatMap(List::stream).collect(Collectors.toList());
    return values.stream()
        .collect(
            Collectors.toMap(
                v -> v.substring(0, 1),
                Collections::singletonList,
                (l, r) ->
                    Stream.concat(l.stream(), r.stream()).sorted().collect(Collectors.toList())));
  }
}
