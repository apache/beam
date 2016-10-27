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

import static org.hamcrest.Matchers.isA;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link EncodabilityEnforcementFactory}.
 */
@RunWith(JUnit4.class)
public class EncodabilityEnforcementFactoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private EncodabilityEnforcementFactory factory = EncodabilityEnforcementFactory.create();
  private BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  private PCollection<Record> inputPCollection;
  private CommittedBundle<Record> inputBundle;
  private PCollection<Record> outputPCollection;

  @Before
  public void setup() {
    Pipeline p = TestPipeline.create();
    inputPCollection = p.apply(Create.of(new Record()).withCoder(new RecordNoDecodeCoder()));
    outputPCollection = inputPCollection.apply(ParDo.of(new IdentityDoFn()));

    inputBundle =
        bundleFactory
            .<Record>createRootBundle()
            .add(WindowedValue.valueInGlobalWindow(new Record()))
            .commit(Instant.now());
  }

  @Test
  public void encodeFailsThrows() {
    WindowedValue<Record> record = WindowedValue.valueInGlobalWindow(new Record());

    ModelEnforcement<Record> enforcement = createEnforcement(new RecordNoEncodeCoder(), record);

    UncommittedBundle<Record> output =
        bundleFactory.createBundle(outputPCollection).add(record);

    enforcement.beforeElement(record);
    enforcement.afterElement(record);
    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Encode not allowed");
    enforcement.afterFinish(
        inputBundle,
        StepTransformResult.withoutHold(outputPCollection.getProducingTransformInternal())
            .addOutput(output)
            .build(),
        Collections.<CommittedBundle<?>>singleton(output.commit(Instant.now())));
  }

  @Test
  public void decodeFailsThrows() {
    WindowedValue<Record> record = WindowedValue.valueInGlobalWindow(new Record());

    ModelEnforcement<Record> enforcement = createEnforcement(new RecordNoDecodeCoder(), record);

    UncommittedBundle<Record> output =
        bundleFactory.createBundle(outputPCollection).add(record);

    enforcement.beforeElement(record);
    enforcement.afterElement(record);
    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Decode not allowed");
    enforcement.afterFinish(
        inputBundle,
        StepTransformResult.withoutHold(outputPCollection.getProducingTransformInternal())
            .addOutput(output)
            .build(),
        Collections.<CommittedBundle<?>>singleton(output.commit(Instant.now())));
  }

  @Test
  public void consistentWithEqualsStructuralValueNotEqualThrows() {
    WindowedValue<Record> record =
        WindowedValue.<Record>valueInGlobalWindow(
            new Record() {
              @Override
              public String toString() {
                return "OriginalRecord";
              }
            });

    ModelEnforcement<Record> enforcement =
        createEnforcement(new RecordStructuralValueCoder(), record);

    UncommittedBundle<Record> output =
        bundleFactory.createBundle(outputPCollection).add(record);

    enforcement.beforeElement(record);
    enforcement.afterElement(record);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(IllegalArgumentException.class));
    thrown.expectMessage("does not maintain structural value equality");
    thrown.expectMessage(RecordStructuralValueCoder.class.getSimpleName());
    thrown.expectMessage("OriginalRecord");
    enforcement.afterFinish(
        inputBundle,
        StepTransformResult.withoutHold(outputPCollection.getProducingTransformInternal())
            .addOutput(output)
            .build(),
        Collections.<CommittedBundle<?>>singleton(output.commit(Instant.now())));
  }

  @Test
  public void notConsistentWithEqualsStructuralValueNotEqualSucceeds() {
    outputPCollection.setCoder(new RecordNotConsistentWithEqualsStructuralValueCoder());
    WindowedValue<Record> record = WindowedValue.<Record>valueInGlobalWindow(new Record());

    ModelEnforcement<Record> enforcement =
        factory.forBundle(inputBundle, outputPCollection.getProducingTransformInternal());

    UncommittedBundle<Record> output =
        bundleFactory.createBundle(outputPCollection).add(record);

    enforcement.beforeElement(record);
    enforcement.afterElement(record);
    enforcement.afterFinish(
        inputBundle,
        StepTransformResult.withoutHold(outputPCollection.getProducingTransformInternal())
            .addOutput(output)
            .build(),
        Collections.<CommittedBundle<?>>singleton(output.commit(Instant.now())));
  }

  private ModelEnforcement<Record> createEnforcement(
      Coder<Record> coder, WindowedValue<Record> record) {
    TestPipeline p = TestPipeline.create();
    PCollection<Record> unencodable = p.apply(Create.<Record>of().withCoder(coder));
    outputPCollection =
        unencodable.apply(
            MapElements.via(new SimpleIdentity()));
    AppliedPTransform<?, ?, ?> consumer = outputPCollection.getProducingTransformInternal();
    CommittedBundle<Record> input =
        bundleFactory.createBundle(unencodable).add(record).commit(Instant.now());
    ModelEnforcement<Record> enforcement = factory.forBundle(input, consumer);
    return enforcement;
  }

  @Test
  public void structurallyEqualResultsSucceeds() {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> unencodable = p.apply(Create.of(1).withCoder(VarIntCoder.of()));
    AppliedPTransform<?, ?, ?> consumer =
        unencodable.apply(Count.<Integer>globally()).getProducingTransformInternal();

    WindowedValue<Integer> value = WindowedValue.valueInGlobalWindow(1);

    CommittedBundle<Integer> input =
        bundleFactory.createBundle(unencodable).add(value).commit(Instant.now());
    ModelEnforcement<Integer> enforcement = factory.forBundle(input, consumer);

    enforcement.beforeElement(value);
    enforcement.afterElement(value);
    enforcement.afterFinish(
        input,
        StepTransformResult.withoutHold(consumer).build(),
        Collections.<CommittedBundle<?>>emptyList());
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
