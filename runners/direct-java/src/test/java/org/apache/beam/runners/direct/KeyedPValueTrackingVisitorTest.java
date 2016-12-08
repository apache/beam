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

import java.util.Collections;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link KeyedPValueTrackingVisitor}.
 */
@RunWith(JUnit4.class)
public class KeyedPValueTrackingVisitorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private KeyedPValueTrackingVisitor visitor;
  @Rule
  public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Before
  public void setup() {
    p = TestPipeline.create();
    visitor = KeyedPValueTrackingVisitor.create();
  }

  @Test
  public void groupByKeyProducesKeyedOutput() {
    PCollection<KV<String, Iterable<Integer>>> keyed =
        p.apply(Create.of(KV.of("foo", 3)))
            .apply(GroupByKey.<String, Integer>create());

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
  public void keyedInputWithoutKeyPreserving() {
    PCollection<KV<String, Iterable<Integer>>> onceKeyed =
        p.apply(Create.of(KV.of("hello", 42)))
            .apply(GroupByKey.<String, Integer>create())
            .apply(ParDo.of(new IdentityFn<KV<String, Iterable<Integer>>>()));

    p.traverseTopologically(visitor);
    assertThat(visitor.getKeyedPValues(), not(hasItem(onceKeyed)));
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

  private static class IdentityFn<K> extends DoFn<K, K> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element());
    }
  }
}
