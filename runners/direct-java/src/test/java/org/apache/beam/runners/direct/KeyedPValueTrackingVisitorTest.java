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

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.Set;

/**
 * Tests for {@link KeyedPValueTrackingVisitor}.
 */
@RunWith(JUnit4.class)
public class KeyedPValueTrackingVisitorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private KeyedPValueTrackingVisitor visitor;
  private Pipeline p;

  @Before
  public void setup() {
    p = TestPipeline.create();
    @SuppressWarnings("rawtypes")
    Set<Class<? extends PTransform>> producesKeyed =
        ImmutableSet.<Class<? extends PTransform>>of(PrimitiveKeyer.class, CompositeKeyer.class);
    visitor = KeyedPValueTrackingVisitor.create(producesKeyed);
  }

  @Test
  public void primitiveProducesKeyedOutputUnkeyedInputKeyedOutput() {
    PCollection<Integer> keyed =
        p.apply(Create.<Integer>of(1, 2, 3)).apply(new PrimitiveKeyer<Integer>());

    p.traverseTopologically(visitor);
    assertThat(visitor.getKeyedPValues(), hasItem(keyed));
  }

  @Test
  public void primitiveProducesKeyedOutputKeyedInputKeyedOutut() {
    PCollection<Integer> keyed =
        p.apply(Create.<Integer>of(1, 2, 3))
            .apply("firstKey", new PrimitiveKeyer<Integer>())
            .apply("secondKey", new PrimitiveKeyer<Integer>());

    p.traverseTopologically(visitor);
    assertThat(visitor.getKeyedPValues(), hasItem(keyed));
  }

  @Test
  public void compositeProducesKeyedOutputUnkeyedInputKeyedOutput() {
    PCollection<Integer> keyed =
        p.apply(Create.<Integer>of(1, 2, 3)).apply(new CompositeKeyer<Integer>());

    p.traverseTopologically(visitor);
    assertThat(visitor.getKeyedPValues(), hasItem(keyed));
  }

  @Test
  public void compositeProducesKeyedOutputKeyedInputKeyedOutut() {
    PCollection<Integer> keyed =
        p.apply(Create.<Integer>of(1, 2, 3))
            .apply("firstKey", new CompositeKeyer<Integer>())
            .apply("secondKey", new CompositeKeyer<Integer>());

    p.traverseTopologically(visitor);
    assertThat(visitor.getKeyedPValues(), hasItem(keyed));
  }


  @Test
  public void noInputUnkeyedOutput() {
    PCollection<KV<Integer, Iterable<Void>>> unkeyed =
        p.apply(
            Create.of(KV.<Integer, Iterable<Void>>of(-1, Collections.<Void>emptyList()))
                .withCoder(KvCoder.of(VarIntCoder.of(), IterableCoder.of(VoidCoder.of()))));

    p.traverseTopologically(visitor);
    assertThat(visitor.getKeyedPValues(), not(hasItem(unkeyed)));
  }

  @Test
  public void keyedInputNotProducesKeyedOutputUnkeyedOutput() {
    PCollection<Integer> onceKeyed =
        p.apply(Create.<Integer>of(1, 2, 3))
            .apply(new PrimitiveKeyer<Integer>())
            .apply(ParDo.of(new IdentityFn<Integer>()));

    p.traverseTopologically(visitor);
    assertThat(visitor.getKeyedPValues(), not(hasItem(onceKeyed)));
  }

  @Test
  public void unkeyedInputNotProducesKeyedOutputUnkeyedOutput() {
    PCollection<Integer> unkeyed =
        p.apply(Create.<Integer>of(1, 2, 3)).apply(ParDo.of(new IdentityFn<Integer>()));

    p.traverseTopologically(visitor);
    assertThat(visitor.getKeyedPValues(), not(hasItem(unkeyed)));
  }

  @Test
  public void traverseMultipleTimesThrows() {
    p.apply(
            Create.<KV<Integer, Void>>of(
                    KV.of(1, (Void) null), KV.of(2, (Void) null), KV.of(3, (Void) null))
                .withCoder(KvCoder.of(VarIntCoder.of(), VoidCoder.of())))
        .apply(GroupByKey.<Integer, Void>create())
        .apply(Keys.<Integer>create());

    p.traverseTopologically(visitor);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("already been finalized");
    thrown.expectMessage(KeyedPValueTrackingVisitor.class.getSimpleName());
    p.traverseTopologically(visitor);
  }

  @Test
  public void getKeyedPValuesBeforeTraverseThrows() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("completely traversed");
    thrown.expectMessage("getKeyedPValues");
    visitor.getKeyedPValues();
  }

  private static class PrimitiveKeyer<K> extends PTransform<PCollection<K>, PCollection<K>> {
    @Override
    public PCollection<K> apply(PCollection<K> input) {
      return PCollection.<K>createPrimitiveOutputInternal(
              input.getPipeline(), input.getWindowingStrategy(), input.isBounded())
          .setCoder(input.getCoder());
    }
  }

  private static class CompositeKeyer<K> extends PTransform<PCollection<K>, PCollection<K>> {
    @Override
    public PCollection<K> apply(PCollection<K> input) {
      return input.apply(new PrimitiveKeyer<K>()).apply(ParDo.of(new IdentityFn<K>()));
    }
  }

  private static class IdentityFn<K> extends OldDoFn<K, K> {
    @Override
    public void processElement(OldDoFn<K, K>.ProcessContext c) throws Exception {
      c.output(c.element());
    }
  }
}
