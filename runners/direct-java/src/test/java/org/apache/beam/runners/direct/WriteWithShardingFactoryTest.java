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

package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.beam.runners.direct.WriteWithShardingFactory.CalculateShardsFn;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.DynamicDestinationHelpers.ConstantFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link WriteWithShardingFactory}.
 */
@RunWith(JUnit4.class)
public class WriteWithShardingFactoryTest implements Serializable {

  private static final int INPUT_SIZE = 10000;

  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();

  private transient WriteWithShardingFactory<Object> factory = new WriteWithShardingFactory<>();

  @Rule
  public final transient TestPipeline p =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void dynamicallyReshardedWrite() throws Exception {
    List<String> strs = new ArrayList<>(INPUT_SIZE);
    for (int i = 0; i < INPUT_SIZE; i++) {
      strs.add(UUID.randomUUID().toString());
    }
    Collections.shuffle(strs);

    String fileName = "resharded_write";
    String targetLocation = tmp.getRoot().toPath().resolve(fileName).toString();
    String targetLocationGlob = targetLocation + '*';

    // TextIO is implemented in terms of the WriteFiles PTransform. When sharding is not specified,
    // resharding should be automatically applied
    p.apply(Create.of(strs)).apply(TextIO.write().to(targetLocation));
    p.run();

    List<Metadata> matches = FileSystems.match(targetLocationGlob).metadata();
    List<String> actuals = new ArrayList<>(strs.size());
    List<String> files = new ArrayList<>(strs.size());
    for (Metadata match : matches) {
      String filename = match.resourceId().toString();
      files.add(filename);
      CharBuffer buf = CharBuffer.allocate((int) new File(filename).length());
      try (Reader reader = new FileReader(filename)) {
        reader.read(buf);
        buf.flip();
      }

      String[] readStrs = buf.toString().split("\n");
      for (String read : readStrs) {
        if (read.length() > 0) {
          actuals.add(read);
        }
      }
    }

    assertThat(actuals, containsInAnyOrder(strs.toArray()));
    assertThat(
        files,
        hasSize(
            allOf(
                greaterThan(1),
                lessThan(
                    (int)
                        (Math.log10(INPUT_SIZE)
                            + WriteWithShardingFactory.MAX_RANDOM_EXTRA_SHARDS)))));
  }

  @Test
  public void withNoShardingSpecifiedReturnsNewTransform() {
    ResourceId outputDirectory = LocalResources.fromString("/foo", true /* isDirectory */);

    PTransform<PCollection<Object>, PDone> original =
        WriteFiles.to(
            new FileBasedSink<Object, Void>(StaticValueProvider.of(outputDirectory),
                new ConstantFilenamePolicy<>(null)) {
              @Override
              public WriteOperation<Object, Void> createWriteOperation() {
                throw new IllegalArgumentException("Should not be used");
              }
            },
            new SerializableFunction<Object, Object>() {
              @Override
              public Object apply(Object input) {
                return input;
              }
            });
    @SuppressWarnings("unchecked")
    PCollection<Object> objs = (PCollection) p.apply(Create.empty(VoidCoder.of()));

    AppliedPTransform<PCollection<Object>, PDone, PTransform<PCollection<Object>, PDone>>
        originalApplication =
            AppliedPTransform.of(
                "write", objs.expand(), Collections.<TupleTag<?>, PValue>emptyMap(), original, p);

    assertThat(
        factory.getReplacementTransform(originalApplication).getTransform(),
        not(equalTo((Object) original)));
  }

  @Test
  public void keyBasedOnCountFnWithNoElements() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> outputs = fnTester.processBundle(0L);
    assertThat(
        outputs, containsInAnyOrder(1));
  }

  @Test
  public void keyBasedOnCountFnWithOneElement() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> outputs = fnTester.processBundle(1L);
    assertThat(
        outputs, containsInAnyOrder(1));
  }

  @Test
  public void keyBasedOnCountFnWithTwoElements() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> outputs = fnTester.processBundle(2L);
    assertThat(outputs, containsInAnyOrder(2));
  }

  @Test
  public void keyBasedOnCountFnFewElementsThreeShards() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> outputs = fnTester.processBundle(5L);
    assertThat(outputs, containsInAnyOrder(3));
  }

  @Test
  public void keyBasedOnCountFnManyElements() throws Exception {
    DoFn<Long, Integer> fn = new CalculateShardsFn(0);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    List<Integer> shard = fnTester.processBundle((long) Math.pow(10, 10));
    assertThat(shard, containsInAnyOrder(10));
  }

  @Test
  public void keyBasedOnCountFnFewElementsExtraShards() throws Exception {
    long countValue = (long) WriteWithShardingFactory.MIN_SHARDS_FOR_LOG + 3;
    PCollection<Long> inputCount = p.apply(Create.of(countValue));
    PCollectionView<Long> elementCountView =
        PCollectionViews.singletonView(
            inputCount, WindowingStrategy.globalDefault(), true, 0L, VarLongCoder.of());
    CalculateShardsFn fn = new CalculateShardsFn(3);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    fnTester.setSideInput(elementCountView, GlobalWindow.INSTANCE, countValue);

    List<Integer> kvs = fnTester.processBundle(10L);
    assertThat(kvs, containsInAnyOrder(6));
  }

  @Test
  public void keyBasedOnCountFnManyElementsExtraShards() throws Exception {
    CalculateShardsFn fn = new CalculateShardsFn(3);
    DoFnTester<Long, Integer> fnTester = DoFnTester.of(fn);

    double count = Math.pow(10, 10);

    List<Integer> shards = fnTester.processBundle((long) count);
    assertThat(shards, containsInAnyOrder(13));
  }
}
