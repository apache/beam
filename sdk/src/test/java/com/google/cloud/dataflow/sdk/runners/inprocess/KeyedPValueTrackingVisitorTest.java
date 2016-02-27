/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.inprocess.GroupByKeyEvaluatorFactory.InProcessGroupByKeyOnly;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.Keys;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.common.collect.ImmutableSet;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Tests for {@link KeyedPValueTrackingVisitor}.
 */
@RunWith(JUnit4.class)
public class KeyedPValueTrackingVisitorTest {
  private KeyedPValueTrackingVisitor visitor;
  private Pipeline p;

  @Before
  public void setup() {
    PipelineOptions options = PipelineOptionsFactory.create();
    // Overrides for GroupByKey, specifically
    options.setRunner(InProcessPipelineRunner.class);

    p = Pipeline.create(options);
    @SuppressWarnings("rawtypes")
    Set<Class<? extends PTransform>> producesKeyed =
        ImmutableSet.<Class<? extends PTransform>>of(InProcessGroupByKeyOnly.class);
    visitor = KeyedPValueTrackingVisitor.create(producesKeyed);
  }

  @Test
  public void singleInputTransformKeyedInputKeyedOutput() {
    PCollection<KV<Integer, Iterable<Void>>> keyed =
        p.apply(
                Create.<KV<Integer, Void>>of(
                        KV.of(1, (Void) null), KV.of(2, (Void) null), KV.of(3, (Void) null))
                    .withCoder(KvCoder.of(VarIntCoder.of(), VoidCoder.of())))
            .apply(GroupByKey.<Integer, Void>create());

    PCollection<Integer> keys = keyed.apply(Keys.<Integer>create());

    p.traverseTopologically(visitor);

    Set<PValue> keyedPValues = visitor.getKeyedPValues();
    assertThat(keyedPValues, Matchers.<PValue>hasItem(keys));
  }

  @Test
  public void singleInputTransformUnkeyedInputUnkeyedOutput() {
    PCollection<KV<Integer, Iterable<Void>>> unkeyed =
        p.apply(
            Create.of(KV.<Integer, Iterable<Void>>of(-1, Collections.<Void>emptyList()))
                .withCoder(KvCoder.of(VarIntCoder.of(), IterableCoder.of(VoidCoder.of()))));

    p.traverseTopologically(visitor);

    Set<PValue> keyedPValues = visitor.getKeyedPValues();
    assertThat(keyedPValues, not(Matchers.<PValue>hasItem(unkeyed)));
  }

  @Test
  public void multiOutputTransformKeyedInputKeyedOutputs() {
    PCollection<KV<Integer, Iterable<Void>>> keyed =
        p.apply(
                Create.<KV<Integer, Void>>of(
                        KV.of(1, (Void) null), KV.of(2, (Void) null), KV.of(3, (Void) null))
                    .withCoder(KvCoder.of(VarIntCoder.of(), VoidCoder.of())))
            .apply(GroupByKey.<Integer, Void>create());

    TupleTag<Integer> mainOutputTag = new TupleTag<Integer>() {};
    TupleTag<Iterable<Void>> sideOutputTag = new TupleTag<Iterable<Void>>() {};
    TupleTag<KV<Integer, Iterable<Void>>> otherSideOutputTag =
        new TupleTag<KV<Integer, Iterable<Void>>>() {};
    TupleTagList sideOutputTags = TupleTagList.of(sideOutputTag).and(otherSideOutputTag);
    PCollectionTuple outputs =
        keyed.apply(ParDo.withOutputTags(mainOutputTag, sideOutputTags).of(new OutputMultiDoFn()));

    p.traverseTopologically(visitor);

    Set<PValue> keyedPValues = visitor.getKeyedPValues();

    Collection<PValue> allOutputs = (Collection<PValue>) outputs.expand();
    assertThat(keyedPValues, Matchers.<PValue>hasItems(allOutputs.toArray(new PValue[0])));
  }

  @Test
  public void multiOutputTransformUnkeyedInputUnkeyedOutputs() {
    PCollection<KV<Integer, Iterable<Void>>> unkeyed =
        p.apply(
            Create.of(KV.<Integer, Iterable<Void>>of(-1, Collections.<Void>emptyList()))
                .withCoder(KvCoder.of(VarIntCoder.of(), IterableCoder.of(VoidCoder.of()))));


    TupleTag<Integer> mainOutputTag = new TupleTag<Integer>() {};
    TupleTag<Iterable<Void>> sideOutputTag = new TupleTag<Iterable<Void>>() {};
    TupleTag<KV<Integer, Iterable<Void>>> otherSideOutputTag =
        new TupleTag<KV<Integer, Iterable<Void>>>() {};
    TupleTagList sideOutputTags = TupleTagList.of(sideOutputTag).and(otherSideOutputTag);
    PCollectionTuple outputs =
        unkeyed.apply(
            ParDo.withOutputTags(mainOutputTag, sideOutputTags).of(new OutputMultiDoFn()));

    p.traverseTopologically(visitor);

    Set<PValue> keyedPValues = visitor.getKeyedPValues();

    Collection<PValue> allOutputs = (Collection<PValue>) outputs.expand();
    assertThat(keyedPValues, not(Matchers.<PValue>hasItems(allOutputs.toArray(new PValue[0]))));
  }

  @Test
  public void multiInputTransformAllUnkeyedUnkeyedOutput() {
    PCollection<KV<Integer, Iterable<Void>>> unkeyed =
        p.apply(
            Create.<KV<Integer, Iterable<Void>>>of(
                    KV.<Integer, Iterable<Void>>of(2, Collections.<Void>emptyList()))
                .withCoder(KvCoder.of(VarIntCoder.of(), IterableCoder.of(VoidCoder.of()))));

    PCollection<KV<Integer, Iterable<Void>>> alsoUnkeyed =
        p.apply(
            Create.of(KV.<Integer, Iterable<Void>>of(-1, Collections.<Void>emptyList()))
                .withCoder(KvCoder.of(VarIntCoder.of(), IterableCoder.of(VoidCoder.of()))));

    PCollection<KV<Integer, Iterable<Void>>> fromAllUnkeyed =
        PCollectionList.of(unkeyed)
            .and(alsoUnkeyed)
            .apply(Flatten.<KV<Integer, Iterable<Void>>>pCollections());

    p.traverseTopologically(visitor);

    Set<PValue> keyedPValues = visitor.getKeyedPValues();
    assertThat(keyedPValues, not(Matchers.<PValue>hasItem(fromAllUnkeyed)));
  }

  @Test
  public void multiInputTransformPartiallyKeyedUnkeyedOutput() {
    PCollection<KV<Integer, Iterable<Void>>> keyed =
        p.apply(
                Create.<KV<Integer, Void>>of(
                        KV.of(1, (Void) null), KV.of(2, (Void) null), KV.of(3, (Void) null))
                    .withCoder(KvCoder.of(VarIntCoder.of(), VoidCoder.of())))
            .apply(GroupByKey.<Integer, Void>create());

    PCollection<KV<Integer, Iterable<Void>>> unkeyed =
        p.apply(
            Create.of(KV.<Integer, Iterable<Void>>of(-1, Collections.<Void>emptyList()))
                .withCoder(KvCoder.of(VarIntCoder.of(), IterableCoder.of(VoidCoder.of()))));

    PCollection<KV<Integer, Iterable<Void>>> partiallyKeyed =
        PCollectionList.of(keyed)
            .and(unkeyed)
            .apply(Flatten.<KV<Integer, Iterable<Void>>>pCollections());

    p.traverseTopologically(visitor);

    Set<PValue> keyedPValues = visitor.getKeyedPValues();
    assertThat(keyedPValues, not(Matchers.<PValue>hasItem(partiallyKeyed)));
  }

  @Test
  public void multiInputTransformAllKeyedKeyedOutput() {
    PCollection<KV<Integer, Iterable<Void>>> keyed =
        p.apply(
                Create.<KV<Integer, Void>>of(
                        KV.of(1, (Void) null), KV.of(2, (Void) null), KV.of(3, (Void) null))
                    .withCoder(KvCoder.of(VarIntCoder.of(), VoidCoder.of())))
            .apply(GroupByKey.<Integer, Void>create());

    PCollection<KV<Integer, Iterable<Void>>> alsoKeyed =
        p.apply(
                Create.<KV<Integer, Void>>of(
                        KV.of(-11, (Void) null), KV.of(-12, (Void) null), KV.of(-3, (Void) null))
                    .withCoder(KvCoder.of(VarIntCoder.of(), VoidCoder.of())))
            .apply(GroupByKey.<Integer, Void>create());

    PCollection<KV<Integer, Iterable<Void>>> multiKeyed =
        PCollectionList.of(keyed)
            .and(alsoKeyed)
            .apply(Flatten.<KV<Integer, Iterable<Void>>>pCollections());

    p.traverseTopologically(visitor);

    Set<PValue> keyedPValues = visitor.getKeyedPValues();
    assertThat(keyedPValues, Matchers.<PValue>hasItem(multiKeyed));
  }

  private static class OutputMultiDoFn extends DoFn<KV<Integer, Iterable<Void>>, Integer> {
    @Override
    public void processElement(DoFn<KV<Integer, Iterable<Void>>, Integer>.ProcessContext c)
        throws Exception {}
  }
}
