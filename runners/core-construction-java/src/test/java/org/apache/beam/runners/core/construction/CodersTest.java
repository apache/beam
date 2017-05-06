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
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link Coders}. */
@RunWith(Enclosed.class)
public class CodersTest {
  private static final Set<StructuredCoder<?>> KNOWN_CODERS =
      ImmutableSet.<StructuredCoder<?>>builder()
          .add(ByteArrayCoder.of())
          .add(KvCoder.of(VarLongCoder.of(), VarLongCoder.of()))
          .add(VarLongCoder.of())
          .add(IntervalWindowCoder.of())
          .add(IterableCoder.of(ByteArrayCoder.of()))
          .add(LengthPrefixCoder.of(IterableCoder.of(VarLongCoder.of())))
          .add(GlobalWindow.Coder.INSTANCE)
          .add(
              FullWindowedValueCoder.of(
                  IterableCoder.of(VarLongCoder.of()), IntervalWindowCoder.of()))
          .build();

  /**
   * Tests that all known coders are present in the parameters that will be used by
   * {@link ToFromProtoTest}.
   */
  @RunWith(JUnit4.class)
  public static class ValidateKnownCodersPresentTest {
    @Test
    public void validateKnownCoders() {
      // Validates that every known coder in the Coders class is represented in a "Known Coder"
      // tests, which demonstrates that they are serialized via components and specified URNs rather
      // than java serialized
      Set<Class<? extends StructuredCoder>> knownCoderClasses = Coders.KNOWN_CODER_URNS.keySet();
      Set<Class<? extends StructuredCoder>> knownCoderTests = new HashSet<>();
      for (StructuredCoder<?> coder : KNOWN_CODERS) {
        knownCoderTests.add(coder.getClass());
      }
      Set<Class<? extends StructuredCoder>> missingKnownCoders = new HashSet<>(knownCoderClasses);
      missingKnownCoders.removeAll(knownCoderTests);
      assertThat(
          String.format(
              "Missing validation of known coder %s in %s",
              missingKnownCoders, CodersTest.class.getSimpleName()),
          missingKnownCoders,
          Matchers.empty());
    }

    @Test
    public void validateCoderTranslators() {
      assertThat(
          "Every Known Coder must have a Known Translator",
          Coders.KNOWN_CODER_URNS.keySet(),
          equalTo(Coders.KNOWN_TRANSLATORS.keySet()));
    }
  }


  /**
   * Tests round-trip coder encodings for both known and unknown {@link Coder coders}.
   */
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
              KvCoder.of(new RecordCoder(), AvroCoder.of(Record.class)))
          .build();
    }

    @Parameter(0)
    public Coder<?> coder;

    @Test
    public void toAndFromProto() throws Exception {
      SdkComponents componentsBuilder = SdkComponents.create();
      RunnerApi.Coder coderProto = Coders.toProto(coder, componentsBuilder);

      Components encodedComponents = componentsBuilder.toComponents();
      Coder<?> decodedCoder = Coders.fromProto(coderProto, encodedComponents);
      assertThat(decodedCoder, Matchers.<Coder<?>>equalTo(coder));

      if (KNOWN_CODERS.contains(coder)) {
        for (RunnerApi.Coder encodedCoder : encodedComponents.getCodersMap().values()) {
          assertThat(
              encodedCoder.getSpec().getSpec().getUrn(),
              not(equalTo(Coders.JAVA_SERIALIZED_CODER_URN)));
        }
      }
    }

    static class Record implements Serializable {}

    private static class RecordCoder extends AtomicCoder<Record> {
      @Override
      public void encode(Record value, OutputStream outStream)
          throws CoderException, IOException {}

      @Override
      public Record decode(InputStream inStream)
          throws CoderException, IOException {
        return new Record();
      }
    }
  }
}
