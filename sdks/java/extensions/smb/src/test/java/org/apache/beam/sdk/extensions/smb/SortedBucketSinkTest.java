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

import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link SortedBucketSink}. */
public class SortedBucketSinkTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder output = new TemporaryFolder();
  @Rule public final TemporaryFolder temp = new TemporaryFolder();

  // test input, [a01, a02, ..., a09, a10, b01, ...]
  private static final String[] input =
      Stream.concat(
              IntStream.rangeClosed('a', 'z').boxed(), IntStream.rangeClosed('A', 'Z').boxed())
          .flatMap(
              i ->
                  IntStream.rangeClosed(1, 10)
                      .boxed()
                      .map(j -> String.format("%s%02d", String.valueOf((char) i.intValue()), j)))
          .toArray(String[]::new);

  @Test
  @Category(NeedsRunner.class)
  public void testOneBucketOneShard() throws Exception {
    test(
        1,
        1,
        m -> {
          Assert.assertEquals(1, m.size());
          BucketShardId id = BucketShardId.of(0, 0);
          List<String> actual = m.get(id);
          MatcherAssert.assertThat(actual, Matchers.containsInAnyOrder(input));
        });
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMultiBucketOneShard() throws Exception {
    test(
        2,
        1,
        m -> {
          Assert.assertEquals(2, m.size());
          BucketShardId id1 = BucketShardId.of(0, 0);
          BucketShardId id2 = BucketShardId.of(1, 0);
          List<String> actual1 = m.get(id1);
          List<String> actual2 = m.get(id2);
          MatcherAssert.assertThat(
              Iterables.concat(actual1, actual2), Matchers.containsInAnyOrder(input));

          // Keys in each bucket do not overlap
          Set<String> key1 = getKeys(actual1);
          Set<String> key2 = getKeys(actual2);
          Assert.assertTrue(Sets.intersection(key1, key2).isEmpty());
        });
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOneBucketMultiShard() throws Exception {
    test(
        1,
        2,
        m -> {
          Assert.assertEquals(2, m.size());
          BucketShardId id1 = BucketShardId.of(0, 0);
          BucketShardId id2 = BucketShardId.of(0, 1);
          List<String> actual1 = m.get(id1);
          List<String> actual2 = m.get(id2);
          MatcherAssert.assertThat(
              Iterables.concat(actual1, actual2), Matchers.containsInAnyOrder(input));

          // Keys in each shard can overlap
          Set<String> key1 = getKeys(actual1);
          Set<String> key2 = getKeys(actual2);
          Assert.assertFalse(Sets.intersection(key1, key2).isEmpty());
        });
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMultiBucketMultiShard() throws Exception {
    test(
        2,
        2,
        m -> {
          Assert.assertEquals(4, m.size());
          BucketShardId id1a = BucketShardId.of(0, 0);
          BucketShardId id1b = BucketShardId.of(0, 1);
          BucketShardId id2a = BucketShardId.of(1, 0);
          BucketShardId id2b = BucketShardId.of(1, 1);
          List<String> actual1a = m.get(id1a);
          List<String> actual1b = m.get(id1b);
          List<String> actual2a = m.get(id2a);
          List<String> actual2b = m.get(id2b);
          MatcherAssert.assertThat(
              Iterables.concat(actual1a, actual1b, actual2a, actual2b),
              Matchers.containsInAnyOrder(input));

          Set<String> key1a = getKeys(actual1a);
          Set<String> key1b = getKeys(actual1b);
          Set<String> key2a = getKeys(actual2a);
          Set<String> key2b = getKeys(actual2b);

          // Keys in each bucket do not overlap
          Assert.assertTrue(Sets.intersection(key1a, key2a).isEmpty());
          Assert.assertTrue(Sets.intersection(key1a, key2b).isEmpty());
          Assert.assertTrue(Sets.intersection(key1b, key2a).isEmpty());
          Assert.assertTrue(Sets.intersection(key1b, key2b).isEmpty());

          // Keys in each shard can overlap
          Assert.assertFalse(Sets.intersection(key1a, key1b).isEmpty());
          Assert.assertFalse(Sets.intersection(key2a, key2b).isEmpty());
        });
  }

  private void test(
      int numBuckets, int numShards, SerializableConsumer<Map<BucketShardId, List<String>>> checkFn)
      throws Exception {
    final TestBucketMetadata metadata = TestBucketMetadata.of(numBuckets, numShards);

    final SortedBucketSink<String, String> sink =
        new SortedBucketSink<>(
            metadata, fromFolder(output), fromFolder(temp), ".txt", new TestFileOperations(), 1);

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<String> reshuffle = Reshuffle.viaRandomKey();

    final WriteResult writeResult =
        pipeline
            .apply(Create.of(Stream.of(input).collect(Collectors.toList())))
            .apply(reshuffle)
            .apply(sink);

    @SuppressWarnings("unchecked")
    final PCollection<ResourceId> writtenMetadata =
        (PCollection<ResourceId>) writeResult.expand().get(new TupleTag<>("WrittenMetadata"));

    @SuppressWarnings("unchecked")
    final PCollection<KV<BucketShardId, ResourceId>> writtenFiles =
        (PCollection<KV<BucketShardId, ResourceId>>)
            writeResult.expand().get(new TupleTag<>("WrittenFiles"));

    PAssert.thatSingleton(writtenMetadata)
        .satisfies(
            id -> {
              Assert.assertTrue(readMetadata(id).isCompatibleWith(metadata));
              return null;
            });

    PAssert.thatMap(writtenFiles)
        .satisfies(
            m -> {
              final Map<BucketShardId, List<String>> data =
                  m.entrySet().stream()
                      .collect(Collectors.toMap(Map.Entry::getKey, e -> readFile(e.getValue())));
              checkFn.accept(data);
              return null;
            });

    pipeline.run();
  }

  private static BucketMetadata<String, String> readMetadata(ResourceId file) {
    try {
      return BucketMetadata.from(Channels.newInputStream(FileSystems.open(file)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<String> readFile(ResourceId file) {
    try {
      return CharStreams.readLines(
          new InputStreamReader(
              Channels.newInputStream(FileSystems.open(file)), Charset.defaultCharset()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Set<String> getKeys(List<String> content) {
    return content.stream().map(s -> s.substring(0, 1)).collect(Collectors.toSet());
  }

  private interface SerializableConsumer<T> extends Consumer<T>, Serializable {}
}
