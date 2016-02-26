/*
 * Copyright (C) 2015 Google Inc.
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.NullableCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessCreate.InMemorySource;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.SourceTestUtils;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link InProcessCreate}.
 */
@RunWith(JUnit4.class)
public class InProcessCreateTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConvertsCreate() {
    TestPipeline p = TestPipeline.create();
    Create.Values<Integer> og = Create.of(1, 2, 3);

    InProcessCreate<Integer> converted = InProcessCreate.from(og);

    DataflowAssert.that(p.apply(converted)).containsInAnyOrder(2, 1, 3);
  }

  @Test
  public void testConvertsCreateWithNullElements() {
    Create.Values<String> og =
        Create.<String>of("foo", null, "spam", "ham", null, "eggs")
            .withCoder(NullableCoder.of(StringUtf8Coder.of()));

    InProcessCreate<String> converted = InProcessCreate.from(og);
    TestPipeline p = TestPipeline.create();

    DataflowAssert.that(p.apply(converted))
        .containsInAnyOrder(null, "foo", null, "spam", "ham", "eggs");
  }

  static class Record implements Serializable {}

  static class Record2 extends Record {}

  @Test
  public void testThrowsIllegalArgumentWhenCannotInferCoder() {
    Create.Values<Record> og = Create.of(new Record(), new Record2());
    InProcessCreate<Record> converted = InProcessCreate.from(og);

    Pipeline p = TestPipeline.create();

    // Create won't infer a default coder in this case.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(Matchers.containsString("Unable to infer a coder"));

    PCollection<Record> c = p.apply(converted);
    p.run();

    fail("Unexpectedly Inferred Coder " + c.getCoder());
  }

  /**
   * An unserializable class to demonstrate encoding of elements.
   */
  private static class UnserializableRecord {
    private final String myString;

    private UnserializableRecord(String myString) {
      this.myString = myString;
    }

    @Override
    public int hashCode() {
      return myString.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      return myString.equals(((UnserializableRecord) o).myString);
    }

    static class UnserializableRecordCoder extends StandardCoder<UnserializableRecord> {
      private final Coder<String> stringCoder = StringUtf8Coder.of();

      @Override
      public void encode(
          UnserializableRecord value,
          OutputStream outStream,
          com.google.cloud.dataflow.sdk.coders.Coder.Context context)
          throws CoderException, IOException {
        stringCoder.encode(value.myString, outStream, context.nested());
      }

      @Override
      public UnserializableRecord decode(
          InputStream inStream, com.google.cloud.dataflow.sdk.coders.Coder.Context context)
          throws CoderException, IOException {
        return new UnserializableRecord(stringCoder.decode(inStream, context.nested()));
      }

      @Override
      public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
      }

      @Override
      public void verifyDeterministic() throws Coder.NonDeterministicException {
        stringCoder.verifyDeterministic();
      }
    }
  }

  @Test
  public void testSerializableOnUnserializableElements() throws Exception {
    List<UnserializableRecord> elements =
        ImmutableList.of(
            new UnserializableRecord("foo"),
            new UnserializableRecord("bar"),
            new UnserializableRecord("baz"));
    InMemorySource<UnserializableRecord> source =
        new InMemorySource<>(elements, new UnserializableRecord.UnserializableRecordCoder());
    SerializableUtils.ensureSerializable(source);
  }

  @Test
  public void testSplitIntoBundles() throws Exception {
    InProcessCreate.InMemorySource<Integer> source =
        new InMemorySource<>(ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8), BigEndianIntegerCoder.of());
    PipelineOptions options = PipelineOptionsFactory.create();
    List<? extends BoundedSource<Integer>> splitSources = source.splitIntoBundles(12, options);
    assertThat(splitSources, hasSize(3));
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splitSources, options);
  }

  @Test
  public void testDoesNotProduceSortedKeys() throws Exception {
    InProcessCreate.InMemorySource<String> source =
        new InMemorySource<>(ImmutableList.of("spam", "ham", "eggs"), StringUtf8Coder.of());
    assertThat(source.producesSortedKeys(PipelineOptionsFactory.create()), is(false));
  }

  @Test
  public void testGetDefaultOutputCoderReturnsConstructorCoder() throws Exception {
    Coder<Integer> coder = VarIntCoder.of();
    InProcessCreate.InMemorySource<Integer> source =
        new InMemorySource<>(ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8), coder);

    Coder<Integer> defaultCoder = source.getDefaultOutputCoder();
    assertThat(defaultCoder, equalTo(coder));
  }
}
