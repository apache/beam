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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link CoderTranslation}. */
public class CoderTranslationTest {
  private static final Set<Coder<?>> KNOWN_CODERS =
      ImmutableSet.<Coder<?>>builder()
          .add(ByteArrayCoder.of())
          .add(BooleanCoder.of())
          .add(KvCoder.of(VarLongCoder.of(), VarLongCoder.of()))
          .add(VarLongCoder.of())
          .add(StringUtf8Coder.of())
          .add(IntervalWindowCoder.of())
          .add(IterableCoder.of(ByteArrayCoder.of()))
          .add(Timer.Coder.of(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE))
          .add(LengthPrefixCoder.of(IterableCoder.of(VarLongCoder.of())))
          .add(GlobalWindow.Coder.INSTANCE)
          .add(
              FullWindowedValueCoder.of(
                  IterableCoder.of(VarLongCoder.of()), IntervalWindowCoder.of()))
          .add(WindowedValue.ParamWindowedValueCoder.of(IterableCoder.of(VarLongCoder.of())))
          .add(DoubleCoder.of())
          .add(
              RowCoder.of(
                  Schema.of(
                      Field.of("i16", FieldType.INT16),
                      Field.of("array", FieldType.array(FieldType.STRING)),
                      Field.of("map", FieldType.map(FieldType.STRING, FieldType.INT32)),
                      Field.of("bar", FieldType.logicalType(FixedBytes.of(123))))))
          .build();

  /**
   * Tests that all known coders are present in the parameters that will be used by {@link
   * ToFromProtoTest}.
   */
  @RunWith(JUnit4.class)
  public static class ValidateKnownCodersPresentTest {
    @Test
    public void validateKnownCoders() {
      // Validates that every known coder in the Coders class is represented in a "Known Coder"
      // tests, which demonstrates that they are serialized via components and specified URNs rather
      // than java serialized
      Set<Class<? extends Coder>> knownCoderClasses =
          ModelCoderRegistrar.BEAM_MODEL_CODER_URNS.keySet();
      Set<Class<? extends Coder>> knownCoderTests = new HashSet<>();
      for (Coder<?> coder : KNOWN_CODERS) {
        knownCoderTests.add(coder.getClass());
      }
      Set<Class<? extends Coder>> missingKnownCoders = new HashSet<>(knownCoderClasses);
      missingKnownCoders.removeAll(knownCoderTests);
      assertThat(
          String.format(
              "Missing validation of known coder %s in %s",
              missingKnownCoders, CoderTranslationTest.class.getSimpleName()),
          missingKnownCoders,
          Matchers.empty());
    }

    @Test
    public void validateCoderTranslators() {
      assertThat(
          "Every Model Coder must have a Translator",
          ModelCoderRegistrar.BEAM_MODEL_CODER_URNS.keySet(),
          equalTo(ModelCoderRegistrar.BEAM_MODEL_CODERS.keySet()));
      assertThat(
          "All Model Coders should be registered",
          CoderTranslation.KNOWN_TRANSLATORS.keySet(),
          hasItems(ModelCoderRegistrar.BEAM_MODEL_CODERS.keySet().toArray(new Class[0])));
    }
  }

  /** Tests round-trip coder encodings for both known and unknown {@link Coder coders}. */
  @RunWith(Parameterized.class)
  public static class ToFromProtoTest {
    @Parameters(name = "{index}: {0}")
    public static Iterable<Coder<?>> data() {
      return ImmutableList.<Coder<?>>builder()
          .addAll(KNOWN_CODERS)
          .add(
              StringUtf8Coder.of(),
              SerializableCoder.of(Record.class),
              new RecordCoder(),
              KvCoder.of(
                  new RecordCoder(),
                  AvroCoder.of(SchemaBuilder.record("record").fields().endRecord())))
          .add(
              StringUtf8Coder.of(),
              SerializableCoder.of(Record.class),
              new RecordCoder(),
              KvCoder.of(new RecordCoder(), AvroCoder.of(Record.class)))
          .build();
    }

    @Parameter(0)
    public Coder<?> coder;

    @Test
    public void toAndFromProto() throws Exception {
      SdkComponents sdkComponents = SdkComponents.create();
      sdkComponents.registerEnvironment(Environments.createDockerEnvironment("java"));
      RunnerApi.Coder coderProto = CoderTranslation.toProto(coder, sdkComponents);

      Components encodedComponents = sdkComponents.toComponents();
      Coder<?> decodedCoder =
          CoderTranslation.fromProto(
              coderProto,
              RehydratedComponents.forComponents(encodedComponents),
              TranslationContext.DEFAULT);
      assertThat(decodedCoder, equalTo(coder));

      if (KNOWN_CODERS.contains(coder)) {
        for (RunnerApi.Coder encodedCoder : encodedComponents.getCodersMap().values()) {
          assertThat(
              encodedCoder.getSpec().getUrn(),
              not(equalTo(CoderTranslation.JAVA_SERIALIZED_CODER_URN)));
        }
      }
    }

    static class Record implements Serializable {}

    private static class RecordCoder extends AtomicCoder<Record> {
      @Override
      public void encode(Record value, OutputStream outStream) throws CoderException, IOException {}

      @Override
      public Record decode(InputStream inStream) throws CoderException, IOException {
        return new Record();
      }
    }
  }
}
