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
package org.apache.beam.runners.flink.adapter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Test;

public class BeamFlinkDataSetAdapterTest {

  private static PTransform<PCollection<? extends String>, PCollection<String>> withPrefix(
      String prefix) {
    return ParDo.of(
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@Element String word, OutputReceiver<String> out) {
            out.output(prefix + word);
          }
        });
  }

  @Test
  public void testApplySimpleTransform() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

    DataSet<String> input = env.fromCollection(ImmutableList.of("a", "b", "c"));
    DataSet<String> result =
        new BeamFlinkDataSetAdapter().applyBeamPTransform(input, withPrefix("x"));

    assertThat(result.collect(), containsInAnyOrder("xa", "xb", "xc"));
  }

  @Test
  public void testApplyCompositeTransform() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

    DataSet<String> input = env.fromCollection(ImmutableList.of("a", "b", "c"));
    DataSet<String> result =
        new BeamFlinkDataSetAdapter()
            .applyBeamPTransform(
                input,
                new PTransform<PCollection<String>, PCollection<String>>() {
                  @Override
                  public PCollection<String> expand(PCollection<String> input) {
                    return input.apply(withPrefix("x")).apply(withPrefix("y"));
                  }
                });

    assertThat(result.collect(), containsInAnyOrder("yxa", "yxb", "yxc"));
  }

  @Test
  public void testApplyMultiInputTransform() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

    DataSet<String> input1 = env.fromCollection(ImmutableList.of("a", "b", "c"));
    DataSet<String> input2 = env.fromCollection(ImmutableList.of("d", "e", "f"));
    DataSet<String> result =
        new BeamFlinkDataSetAdapter()
            .applyBeamPTransform(
                ImmutableMap.of("x", input1, "y", input2),
                new PTransform<PCollectionTuple, PCollection<String>>() {
                  @Override
                  public PCollection<String> expand(PCollectionTuple input) {
                    return PCollectionList.of(input.<String>get("x").apply(withPrefix("x")))
                        .and(input.<String>get("y").apply(withPrefix("y")))
                        .apply(Flatten.pCollections());
                  }
                });

    assertThat(result.collect(), containsInAnyOrder("xa", "xb", "xc", "yd", "ye", "yf"));
  }

  @Test
  public void testApplyMultiOutputTransform() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

    DataSet<String> input = env.fromCollection(ImmutableList.of("a", "b", "c"));
    Map<String, DataSet<?>> result =
        new BeamFlinkDataSetAdapter()
            .applyMultiOutputBeamPTransform(
                input,
                new PTransform<PCollection<String>, PCollectionTuple>() {
                  @Override
                  public PCollectionTuple expand(PCollection<String> input) {
                    return PCollectionTuple.of("x", input.apply(withPrefix("x")))
                        .and("y", input.apply(withPrefix("y")));
                  }
                });

    assertThat(result.get("x").collect(), containsInAnyOrder("xa", "xb", "xc"));
    assertThat(result.get("y").collect(), containsInAnyOrder("ya", "yb", "yc"));
  }

  @Test
  public void testApplyGroupingTransform() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

    DataSet<String> input = env.fromCollection(ImmutableList.of("a", "a", "b"));
    DataSet<KV<String, Long>> result =
        new BeamFlinkDataSetAdapter().applyBeamPTransform(input, Count.perElement());

    assertThat(result.collect(), containsInAnyOrder(KV.of("a", 2L), KV.of("b", 1L)));
  }

  @Test
  public void testCustomCoder() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

    DataSet<String> input = env.fromCollection(ImmutableList.of("a", "b", "c"));
    DataSet<String> result =
        new BeamFlinkDataSetAdapter()
            .applyBeamPTransform(
                input,
                new PTransform<PCollection<String>, PCollection<String>>() {
                  @Override
                  public PCollection<String> expand(PCollection<String> input) {
                    return input.apply(withPrefix("x")).setCoder(new MyCoder());
                  }
                });

    assertThat(result.collect(), containsInAnyOrder("xa", "xb", "xc"));
  }

  private static class MyCoder extends Coder<String> {

    private static final int CUSTOM_MARKER = 3;

    @Override
    public void encode(String value, OutputStream outStream) throws IOException {
      outStream.write(CUSTOM_MARKER);
      StringUtf8Coder.of().encode(value, outStream);
    }

    @Override
    public String decode(InputStream inStream) throws IOException {
      assert inStream.read() == CUSTOM_MARKER;
      return StringUtf8Coder.of().decode(inStream);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }
}
