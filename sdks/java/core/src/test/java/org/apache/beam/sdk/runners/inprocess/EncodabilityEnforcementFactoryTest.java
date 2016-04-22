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
package org.apache.beam.sdk.runners.inprocess;

import static org.hamcrest.Matchers.isA;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;

/**
 * Tests for {@link EncodabilityEnforcementFactory}.
 */
@RunWith(JUnit4.class)
public class EncodabilityEnforcementFactoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private EncodabilityEnforcementFactory factory = EncodabilityEnforcementFactory.create();
  private BundleFactory bundleFactory = InProcessBundleFactory.create();

  @Test
  public void encodeFailsThrows() {
    WindowedValue<Record> record = WindowedValue.valueInGlobalWindow(new Record());

    ModelEnforcement<Record> enforcement = createEnforcement(new RecordNoEncodeCoder(), record);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Encode not allowed");
    enforcement.beforeElement(record);
  }

  @Test
  public void decodeFailsThrows() {
    WindowedValue<Record> record = WindowedValue.valueInGlobalWindow(new Record());

    ModelEnforcement<Record> enforcement = createEnforcement(new RecordNoDecodeCoder(), record);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Decode not allowed");
    enforcement.beforeElement(record);
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

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(IllegalArgumentException.class));
    thrown.expectMessage("does not maintain structural value equality");
    thrown.expectMessage(RecordStructuralValueCoder.class.getSimpleName());
    thrown.expectMessage("OriginalRecord");
    enforcement.beforeElement(record);
  }

  @Test
  public void notConsistentWithEqualsStructuralValueNotEqualSucceeds() {
    TestPipeline p = TestPipeline.create();
    PCollection<Record> unencodable =
        p.apply(
            Create.of(new Record())
                .withCoder(new RecordNotConsistentWithEqualsStructuralValueCoder()));
    AppliedPTransform<?, ?, ?> consumer =
        unencodable.apply(Count.<Record>globally()).getProducingTransformInternal();

    WindowedValue<Record> record = WindowedValue.<Record>valueInGlobalWindow(new Record());

    CommittedBundle<Record> input =
        bundleFactory.createRootBundle(unencodable).add(record).commit(Instant.now());
    ModelEnforcement<Record> enforcement = factory.forBundle(input, consumer);

    enforcement.beforeElement(record);
    enforcement.afterElement(record);
    enforcement.afterFinish(
        input,
        StepTransformResult.withoutHold(consumer).build(),
        Collections.<CommittedBundle<?>>emptyList());
  }

  private <T> ModelEnforcement<T> createEnforcement(Coder<T> coder, WindowedValue<T> record) {
    TestPipeline p = TestPipeline.create();
    PCollection<T> unencodable = p.apply(Create.<T>of().withCoder(coder));
    AppliedPTransform<?, ?, ?> consumer =
        unencodable.apply(Count.<T>globally()).getProducingTransformInternal();
    CommittedBundle<T> input =
        bundleFactory.createRootBundle(unencodable).add(record).commit(Instant.now());
    ModelEnforcement<T> enforcement = factory.forBundle(input, consumer);
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
        bundleFactory.createRootBundle(unencodable).add(value).commit(Instant.now());
    ModelEnforcement<Integer> enforcement = factory.forBundle(input, consumer);

    enforcement.beforeElement(value);
    enforcement.afterElement(value);
    enforcement.afterFinish(
        input,
        StepTransformResult.withoutHold(consumer).build(),
        Collections.<CommittedBundle<?>>emptyList());
  }

  private static class Record {}
  private static class RecordNoEncodeCoder extends AtomicCoder<Record> {

    @Override
    public void encode(
        Record value,
        OutputStream outStream,
        org.apache.beam.sdk.coders.Coder.Context context)
        throws CoderException, IOException {
      throw new CoderException("Encode not allowed");
    }

    @Override
    public Record decode(
        InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
        throws CoderException, IOException {
      return null;
    }
  }

  private static class RecordNoDecodeCoder extends AtomicCoder<Record> {
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

}
