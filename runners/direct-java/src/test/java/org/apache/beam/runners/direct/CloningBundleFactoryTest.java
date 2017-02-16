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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CloningBundleFactory}.
 */
@RunWith(JUnit4.class)
public class CloningBundleFactoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public final TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  private CloningBundleFactory factory = CloningBundleFactory.create();

  @Test
  public void rootBundleSucceedsIgnoresCoder() {
    WindowedValue<Record> one = WindowedValue.valueInGlobalWindow(new Record());
    WindowedValue<Record> two = WindowedValue.valueInGlobalWindow(new Record());
    CommittedBundle<Record> root =
        factory.<Record>createRootBundle().add(one).add(two).commit(Instant.now());

    assertThat(root.getElements(), containsInAnyOrder(one, two));
  }

  @Test
  public void bundleWorkingCoderSucceedsClonesOutput() {
    PCollection<Integer> created = p.apply(Create.of(1, 3).withCoder(VarIntCoder.of()));
    PCollection<KV<String, Integer>> kvs =
        created
            .apply(WithKeys.<String, Integer>of("foo"))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
    WindowedValue<KV<String, Integer>> fooOne = WindowedValue.valueInGlobalWindow(KV.of("foo", 1));
    WindowedValue<KV<String, Integer>> fooThree =
        WindowedValue.valueInGlobalWindow(KV.of("foo", 3));
    CommittedBundle<KV<String, Integer>> bundle =
        factory.createBundle(kvs).add(fooOne).add(fooThree).commit(Instant.now());

    assertThat(bundle.getElements(), containsInAnyOrder(fooOne, fooThree));
    assertThat(
        bundle.getElements(), not(containsInAnyOrder(theInstance(fooOne), theInstance(fooThree))));
    for (WindowedValue<KV<String, Integer>> foo : bundle.getElements()) {
      assertThat(
          foo.getValue(),
          not(anyOf(theInstance(fooOne.getValue()), theInstance(fooThree.getValue()))));
    }
    assertThat(bundle.getPCollection(), equalTo(kvs));
  }

  @Test
  public void keyedBundleWorkingCoderSucceedsClonesOutput() {
    PCollection<Integer> created = p.apply(Create.of(1, 3).withCoder(VarIntCoder.of()));

    PCollection<KV<String, Iterable<Integer>>> keyed =
        created
            .apply(WithKeys.<String, Integer>of("foo"))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
            .apply(GroupByKey.<String, Integer>create());
    WindowedValue<KV<String, Iterable<Integer>>> foos =
        WindowedValue.valueInGlobalWindow(
            KV.<String, Iterable<Integer>>of("foo", ImmutableList.of(1, 3)));
    CommittedBundle<KV<String, Iterable<Integer>>> keyedBundle =
        factory
            .createKeyedBundle(StructuralKey.of("foo", StringUtf8Coder.of()), keyed)
            .add(foos)
            .commit(Instant.now());

    assertThat(keyedBundle.getElements(), containsInAnyOrder(foos));
    assertThat(
        Iterables.getOnlyElement(keyedBundle.getElements()).getValue(),
        not(theInstance(foos.getValue())));
    assertThat(keyedBundle.getPCollection(), equalTo(keyed));
    assertThat(
        keyedBundle.getKey(),
        Matchers.<StructuralKey<?>>equalTo(StructuralKey.of("foo", StringUtf8Coder.of())));
  }

  @Test
  public void bundleEncodeFailsAddFails() {
    PCollection<Record> pc = p.apply(Create.empty(new RecordNoEncodeCoder()));
    UncommittedBundle<Record> bundle = factory.createBundle(pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Encode not allowed");
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }

  @Test
  public void bundleDecodeFailsAddFails() {
    PCollection<Record> pc = p.apply(Create.empty(new RecordNoDecodeCoder()));
    UncommittedBundle<Record> bundle = factory.createBundle(pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Decode not allowed");
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }

  @Test
  public void keyedBundleEncodeFailsAddFails() {
    PCollection<Record> pc = p.apply(Create.empty(new RecordNoEncodeCoder()));
    UncommittedBundle<Record> bundle =
        factory.createKeyedBundle(StructuralKey.of("foo", StringUtf8Coder.of()), pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Encode not allowed");
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }

  @Test
  public void keyedBundleDecodeFailsAddFails() {
    PCollection<Record> pc = p.apply(Create.empty(new RecordNoDecodeCoder()));
    UncommittedBundle<Record> bundle =
        factory.createKeyedBundle(StructuralKey.of("foo", StringUtf8Coder.of()), pc);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Decode not allowed");
    bundle.add(WindowedValue.valueInGlobalWindow(new Record()));
  }

  static class Record {}
  static class RecordNoEncodeCoder extends AtomicCoder<Record> {

    @Override
    public void encode(
        Record value,
        OutputStream outStream,
        org.apache.beam.sdk.coders.Coder.Context context)
        throws IOException {
      throw new CoderException("Encode not allowed");
    }

    @Override
    public Record decode(
        InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
        throws IOException {
      return null;
    }
  }

  static class RecordNoDecodeCoder extends AtomicCoder<Record> {
    @Override
    public void encode(
        Record value,
        OutputStream outStream,
        org.apache.beam.sdk.coders.Coder.Context context)
        throws IOException {}

    @Override
    public Record decode(
        InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
        throws IOException {
      throw new CoderException("Decode not allowed");
    }
  }

  private static class RecordStructuralValueCoder extends AtomicCoder<Record> {
    @Override
    public void encode(
        Record value,
        OutputStream outStream,
        org.apache.beam.sdk.coders.Coder.Context context)
        throws CoderException, IOException {}

    @Override
    public Record decode(
        InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
        throws CoderException, IOException {
      return new Record() {
        @Override
        public String toString() {
          return "DecodedRecord";
        }
      };
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public Object structuralValue(Record value) {
      return value;
    }
  }

  private static class RecordNotConsistentWithEqualsStructuralValueCoder
      extends AtomicCoder<Record> {
    @Override
    public void encode(
        Record value,
        OutputStream outStream,
        org.apache.beam.sdk.coders.Coder.Context context)
        throws CoderException, IOException {}

    @Override
    public Record decode(
        InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
        throws CoderException, IOException {
      return new Record() {
        @Override
        public String toString() {
          return "DecodedRecord";
        }
      };
    }

    @Override
    public boolean consistentWithEquals() {
      return false;
    }

    @Override
    public Object structuralValue(Record value) {
      return value;
    }
  }

  private static class IdentityDoFn extends DoFn<Record, Record> {
    @ProcessElement
    public void proc(ProcessContext ctxt) {
      ctxt.output(ctxt.element());
    }
  }

  private static class SimpleIdentity extends SimpleFunction<Record, Record> {
    @Override
    public Record apply(Record input) {
      return input;
    }
  }
}
