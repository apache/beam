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
package org.apache.beam.sdk.util.construction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ReplacementOutputs}. */
@RunWith(JUnit4.class)
public class ReplacementOutputsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private TestPipeline p = TestPipeline.create();

  private PCollection<Integer> ints =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of());
  private PCollection<Integer> moreInts =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of());
  private PCollection<String> strs =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, StringUtf8Coder.of());

  private PCollection<Integer> replacementInts =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of());
  private PCollection<Integer> moreReplacementInts =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, VarIntCoder.of());
  private PCollection<String> replacementStrs =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED, StringUtf8Coder.of());

  @Test
  public void singletonSucceeds() {
    Map<PCollection<?>, ReplacementOutput> replacements =
        ReplacementOutputs.singleton(PValues.expandValue(ints), replacementInts);

    assertThat(replacements, Matchers.hasKey(replacementInts));

    ReplacementOutput replacement = replacements.get(replacementInts);
    Map.Entry<TupleTag<?>, PValue> taggedInts = Iterables.getOnlyElement(ints.expand().entrySet());
    assertThat(replacement.getOriginal().getTag(), equalTo(taggedInts.getKey()));
    assertThat(replacement.getOriginal().getValue(), equalTo(taggedInts.getValue()));
    assertThat(replacement.getReplacement().getValue(), equalTo(replacementInts));
  }

  @Test
  public void singletonMultipleOriginalsThrows() {
    thrown.expect(IllegalArgumentException.class);
    ReplacementOutputs.singleton(
        ImmutableMap.<TupleTag<?>, PCollection<?>>builder()
            .putAll(PValues.expandValue(ints))
            .putAll(PValues.fullyExpand(moreInts.expand()))
            .build(),
        replacementInts);
  }

  private TupleTag<Integer> intsTag = new TupleTag<>();
  private TupleTag<Integer> moreIntsTag = new TupleTag<>();
  private TupleTag<String> strsTag = new TupleTag<>();

  @Test
  public void taggedSucceeds() {
    PCollectionTuple original =
        PCollectionTuple.of(intsTag, ints).and(strsTag, strs).and(moreIntsTag, moreInts);

    Map<PCollection<?>, ReplacementOutput> replacements =
        ReplacementOutputs.tagged(
            PValues.expandOutput((POutput) original),
            PCollectionTuple.of(strsTag, replacementStrs)
                .and(moreIntsTag, moreReplacementInts)
                .and(intsTag, replacementInts));
    assertThat(
        replacements.keySet(),
        Matchers.containsInAnyOrder(replacementStrs, replacementInts, moreReplacementInts));
    ReplacementOutput intsReplacement = replacements.get(replacementInts);
    ReplacementOutput strsReplacement = replacements.get(replacementStrs);
    ReplacementOutput moreIntsReplacement = replacements.get(moreReplacementInts);

    assertThat(
        intsReplacement,
        equalTo(
            ReplacementOutput.of(
                TaggedPValue.of(intsTag, ints), TaggedPValue.of(intsTag, replacementInts))));
    assertThat(
        strsReplacement,
        equalTo(
            ReplacementOutput.of(
                TaggedPValue.of(strsTag, strs), TaggedPValue.of(strsTag, replacementStrs))));
    assertThat(
        moreIntsReplacement,
        equalTo(
            ReplacementOutput.of(
                TaggedPValue.of(moreIntsTag, moreInts),
                TaggedPValue.of(moreIntsTag, moreReplacementInts))));
  }

  @Test
  public void taggedMissingReplacementThrows() {
    PCollectionTuple original =
        PCollectionTuple.of(intsTag, ints).and(strsTag, strs).and(moreIntsTag, moreInts);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing replacement");
    thrown.expectMessage(intsTag.toString());
    thrown.expectMessage(ints.toString());
    ReplacementOutputs.tagged(
        PValues.expandOutput(original),
        PCollectionTuple.of(strsTag, replacementStrs).and(moreIntsTag, moreReplacementInts));
  }

  @Test
  public void taggedExtraReplacementThrows() {
    PCollectionTuple original = PCollectionTuple.of(intsTag, ints).and(strsTag, strs);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing original output");
    thrown.expectMessage(moreIntsTag.toString());
    thrown.expectMessage(moreReplacementInts.toString());
    ReplacementOutputs.tagged(
        PValues.expandOutput(original),
        PCollectionTuple.of(strsTag, replacementStrs)
            .and(moreIntsTag, moreReplacementInts)
            .and(intsTag, replacementInts));
  }
}
