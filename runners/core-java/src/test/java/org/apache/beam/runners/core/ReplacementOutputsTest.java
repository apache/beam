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

package org.apache.beam.runners.core;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ReplacementOutputs}.
 */
@RunWith(JUnit4.class)
public class ReplacementOutputsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private TestPipeline p = TestPipeline.create();

  private PCollection<Integer> ints =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
  private PCollection<Integer> moreInts =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
  private PCollection<String> strs =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);

  private PCollection<Integer> replacementInts =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
  private PCollection<Integer> moreReplacementInts =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
  private PCollection<String> replacementStrs =
      PCollection.createPrimitiveOutputInternal(
          p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED);

  @Test
  public void singletonSucceeds() {
    Map<PValue, ReplacementOutput> replacements =
        ReplacementOutputs.singleton(ints.expand(), replacementInts);

    assertThat(replacements, Matchers.<PValue>hasKey(replacementInts));

    ReplacementOutput replacement = replacements.get(replacementInts);
    TaggedPValue taggedInts = Iterables.getOnlyElement(ints.expand());
    assertThat(replacement.getOriginal(), equalTo(taggedInts));
    assertThat(replacement.getReplacement().getValue(), Matchers.<PValue>equalTo(replacementInts));
  }

  @Test
  public void singletonMultipleOriginalsThrows() {
    thrown.expect(IllegalArgumentException.class);
    ReplacementOutputs.singleton(
        ImmutableList.copyOf(Iterables.concat(ints.expand(), moreInts.expand())), replacementInts);
  }

  @Test
  public void orderedSucceeds() {
    List<TaggedPValue> originals = PCollectionList.of(ints).and(moreInts).expand();
    Map<PValue, ReplacementOutput> replacements =
        ReplacementOutputs.ordered(
            originals, PCollectionList.of(replacementInts).and(moreReplacementInts));
    assertThat(
        replacements.keySet(),
        Matchers.<PValue>containsInAnyOrder(replacementInts, moreReplacementInts));

    ReplacementOutput intsMapping = replacements.get(replacementInts);
    assertThat(intsMapping.getOriginal().getValue(), Matchers.<PValue>equalTo(ints));
    assertThat(intsMapping.getReplacement().getValue(), Matchers.<PValue>equalTo(replacementInts));

    ReplacementOutput moreIntsMapping = replacements.get(moreReplacementInts);
    assertThat(moreIntsMapping.getOriginal().getValue(), Matchers.<PValue>equalTo(moreInts));
    assertThat(
        moreIntsMapping.getReplacement().getValue(), Matchers.<PValue>equalTo(moreReplacementInts));
  }

  @Test
  public void orderedTooManyReplacements() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("same size");
    ReplacementOutputs.ordered(
        PCollectionList.of(ints).expand(),
        PCollectionList.of(replacementInts).and(moreReplacementInts));
  }

  @Test
  public void orderedTooFewReplacements() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("same size");
    ReplacementOutputs.ordered(
        PCollectionList.of(ints).and(moreInts).expand(), PCollectionList.of(moreReplacementInts));
  }

  private TupleTag<Integer> intsTag = new TupleTag<>();
  private TupleTag<Integer> moreIntsTag = new TupleTag<>();
  private TupleTag<String> strsTag = new TupleTag<>();

  @Test
  public void taggedSucceeds() {
    PCollectionTuple original =
        PCollectionTuple.of(intsTag, ints).and(strsTag, strs).and(moreIntsTag, moreInts);

    Map<PValue, ReplacementOutput> replacements =
        ReplacementOutputs.tagged(
            original.expand(),
            PCollectionTuple.of(strsTag, replacementStrs)
                .and(moreIntsTag, moreReplacementInts)
                .and(intsTag, replacementInts));
    assertThat(
        replacements.keySet(),
        Matchers.<PValue>containsInAnyOrder(replacementStrs, replacementInts, moreReplacementInts));
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

  /**
   * When a call to {@link ReplacementOutputs#tagged(List, POutput)} is made where the first
   * argument contains multiple copies of the same {@link TaggedPValue}, the call succeeds using
   * that mapping.
   */
  @Test
  public void taggedMultipleInstances() {
    List<TaggedPValue> original =
        ImmutableList.of(
            TaggedPValue.of(intsTag, ints),
            TaggedPValue.of(strsTag, strs),
            TaggedPValue.of(intsTag, ints));

    Map<PValue, ReplacementOutput> replacements =
        ReplacementOutputs.tagged(
            original, PCollectionTuple.of(strsTag, replacementStrs).and(intsTag, replacementInts));
    assertThat(
        replacements.keySet(),
        Matchers.<PValue>containsInAnyOrder(replacementStrs, replacementInts));
    ReplacementOutput intsReplacement = replacements.get(replacementInts);
    ReplacementOutput strsReplacement = replacements.get(replacementStrs);

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
  }

  /**
   * When a call to {@link ReplacementOutputs#tagged(List, POutput)} is made where a single tag
   * has multiple {@link PValue PValues} mapped to it, the call fails.
   */
  @Test
  public void taggedMultipleConflictingInstancesThrows() {
    List<TaggedPValue> original =
        ImmutableList.of(
            TaggedPValue.of(intsTag, ints), TaggedPValue.of(intsTag, moreReplacementInts));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("different values");
    thrown.expectMessage(intsTag.toString());
    thrown.expectMessage(ints.toString());
    thrown.expectMessage(moreReplacementInts.toString());
    ReplacementOutputs.tagged(
        original,
        PCollectionTuple.of(strsTag, replacementStrs)
            .and(moreIntsTag, moreReplacementInts)
            .and(intsTag, replacementInts));
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
        original.expand(),
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
        original.expand(),
        PCollectionTuple.of(strsTag, replacementStrs)
            .and(moreIntsTag, moreReplacementInts)
            .and(intsTag, replacementInts));
  }
}
