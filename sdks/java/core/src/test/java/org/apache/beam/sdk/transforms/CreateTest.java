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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.TestUtils.LINES;
import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create.Values.CreateSource;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Tests for Create.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class CreateTest {
  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(RunnableOnService.class)
  public void testCreate() {
    Pipeline p = TestPipeline.create();

    PCollection<String> output =
        p.apply(Create.of(LINES));

    PAssert.that(output)
        .containsInAnyOrder(LINES_ARRAY);
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCreateEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<String> output =
        p.apply(Create.of(NO_LINES)
            .withCoder(StringUtf8Coder.of()));

    PAssert.that(output)
        .containsInAnyOrder(NO_LINES_ARRAY);
    p.run();
  }

  @Test
  public void testCreateEmptyInfersCoder() {
    Pipeline p = TestPipeline.create();

    PCollection<Object> output =
        p.apply(Create.of());

    assertEquals(VoidCoder.of(), output.getCoder());
  }

  static class Record implements Serializable {
  }

  static class Record2 extends Record {
  }

  @Test
  public void testPolymorphicType() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(
        Matchers.containsString("Unable to infer a coder"));

    Pipeline p = TestPipeline.create();

    // Create won't infer a default coder in this case.
    p.apply(Create.of(new Record(), new Record2()));

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCreateWithNullsAndValues() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<String> output =
        p.apply(Create.of(null, "test1", null, "test2", null)
            .withCoder(SerializableCoder.of(String.class)));
    PAssert.that(output)
        .containsInAnyOrder(null, "test1", null, "test2", null);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCreateParameterizedType() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<TimestampedValue<String>> output =
        p.apply(Create.of(
            TimestampedValue.of("a", new Instant(0)),
            TimestampedValue.of("b", new Instant(0))));

    PAssert.that(output)
        .containsInAnyOrder(
            TimestampedValue.of("a", new Instant(0)),
            TimestampedValue.of("b", new Instant(0)));

    p.run();
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

    static class UnserializableRecordCoder extends AtomicCoder<UnserializableRecord> {
      private final Coder<String> stringCoder = StringUtf8Coder.of();

      @Override
      public void encode(
          UnserializableRecord value,
          OutputStream outStream,
          org.apache.beam.sdk.coders.Coder.Context context)
          throws CoderException, IOException {
        stringCoder.encode(value.myString, outStream, context.nested());
      }

      @Override
      public UnserializableRecord decode(
          InputStream inStream, org.apache.beam.sdk.coders.Coder.Context context)
          throws CoderException, IOException {
        return new UnserializableRecord(stringCoder.decode(inStream, context.nested()));
      }
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCreateWithUnserializableElements() throws Exception {
    List<UnserializableRecord> elements =
        ImmutableList.of(
            new UnserializableRecord("foo"),
            new UnserializableRecord("bar"),
            new UnserializableRecord("baz"));
    Create.Values<UnserializableRecord> create =
        Create.of(elements).withCoder(new UnserializableRecord.UnserializableRecordCoder());

    TestPipeline p = TestPipeline.create();
    PAssert.that(p.apply(create))
        .containsInAnyOrder(
            new UnserializableRecord("foo"),
            new UnserializableRecord("bar"),
            new UnserializableRecord("baz"));
    p.run();
  }

  private static class PrintTimestamps extends OldDoFn<String, String> {
    @Override
      public void processElement(ProcessContext c) {
      c.output(c.element() + ":" + c.timestamp().getMillis());
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCreateTimestamped() {
    Pipeline p = TestPipeline.create();

    List<TimestampedValue<String>> data = Arrays.asList(
        TimestampedValue.of("a", new Instant(1L)),
        TimestampedValue.of("b", new Instant(2L)),
        TimestampedValue.of("c", new Instant(3L)));

    PCollection<String> output =
        p.apply(Create.timestamped(data))
        .apply(ParDo.of(new PrintTimestamps()));

    PAssert.that(output)
        .containsInAnyOrder("a:1", "b:2", "c:3");
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCreateTimestampedEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<String> output = p
        .apply(Create.timestamped(new ArrayList<TimestampedValue<String>>())
            .withCoder(StringUtf8Coder.of()));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  public void testCreateTimestampedEmptyInfersCoder() {
    Pipeline p = TestPipeline.create();

    PCollection<Object> output = p
        .apply(Create.timestamped());

    assertEquals(VoidCoder.of(), output.getCoder());
  }

  @Test
  public void testCreateTimestampedPolymorphicType() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(
        Matchers.containsString("Unable to infer a coder"));

    Pipeline p = TestPipeline.create();

    // Create won't infer a default coder in this case.
    PCollection<Record> c = p.apply(Create.timestamped(
        TimestampedValue.of(new Record(), new Instant(0)),
        TimestampedValue.<Record>of(new Record2(), new Instant(0))));

    p.run();

    throw new RuntimeException("Coder: " + c.getCoder());
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCreateWithVoidType() throws Exception {
    Pipeline p = TestPipeline.create();
    PCollection<Void> output = p.apply(Create.of((Void) null, (Void) null));
    PAssert.that(output).containsInAnyOrder((Void) null, (Void) null);
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCreateWithKVVoidType() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<KV<Void, Void>> output = p.apply(Create.of(
        KV.of((Void) null, (Void) null),
        KV.of((Void) null, (Void) null)));

    PAssert.that(output).containsInAnyOrder(
        KV.of((Void) null, (Void) null),
        KV.of((Void) null, (Void) null));

    p.run();
  }

  @Test
  public void testCreateGetName() {
    assertEquals("Create.Values", Create.of(1, 2, 3).getName());
    assertEquals("Create.TimestampedValues", Create.timestamped(Collections.EMPTY_LIST).getName());
  }

  @Test
  public void testSourceIsSerializableWithUnserializableElements() throws Exception {
    List<UnserializableRecord> elements =
        ImmutableList.of(
            new UnserializableRecord("foo"),
            new UnserializableRecord("bar"),
            new UnserializableRecord("baz"));
    CreateSource<UnserializableRecord> source =
        CreateSource.fromIterable(elements, new UnserializableRecord.UnserializableRecordCoder());
    SerializableUtils.ensureSerializable(source);
  }

  @Test
  public void testSourceSplitIntoBundles() throws Exception {
    CreateSource<Integer> source =
        CreateSource.fromIterable(
            ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8), BigEndianIntegerCoder.of());
    PipelineOptions options = PipelineOptionsFactory.create();
    List<? extends BoundedSource<Integer>> splitSources = source.splitIntoBundles(12, options);
    assertThat(splitSources, hasSize(3));
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splitSources, options);
  }

  @Test
  public void testSourceSplitIntoBundlesVoid() throws Exception {
    CreateSource<Void> source =
        CreateSource.fromIterable(
            Lists.<Void>newArrayList(null, null, null, null, null), VoidCoder.of());
    PipelineOptions options = PipelineOptionsFactory.create();
    List<? extends BoundedSource<Void>> splitSources = source.splitIntoBundles(3, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splitSources, options);
  }

  @Test
  public void testSourceSplitIntoBundlesEmpty() throws Exception {
    CreateSource<Integer> source =
        CreateSource.fromIterable(ImmutableList.<Integer>of(), BigEndianIntegerCoder.of());
    PipelineOptions options = PipelineOptionsFactory.create();
    List<? extends BoundedSource<Integer>> splitSources = source.splitIntoBundles(12, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splitSources, options);
  }

  @Test
  public void testSourceDoesNotProduceSortedKeys() throws Exception {
    CreateSource<String> source =
        CreateSource.fromIterable(ImmutableList.of("spam", "ham", "eggs"), StringUtf8Coder.of());
    assertThat(source.producesSortedKeys(PipelineOptionsFactory.create()), is(false));
  }

  @Test
  public void testSourceGetDefaultOutputCoderReturnsConstructorCoder() throws Exception {
    Coder<Integer> coder = VarIntCoder.of();
    CreateSource<Integer> source =
        CreateSource.fromIterable(ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8), coder);

    Coder<Integer> defaultCoder = source.getDefaultOutputCoder();
    assertThat(defaultCoder, equalTo(coder));
  }

  @Test
  public void testSourceSplitAtFraction() throws Exception {
    List<Integer> elements = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 25; i++) {
      elements.add(random.nextInt());
    }
    CreateSource<Integer> source = CreateSource.fromIterable(elements, VarIntCoder.of());

    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }
}
