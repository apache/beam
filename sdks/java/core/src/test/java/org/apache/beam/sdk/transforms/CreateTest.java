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
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create.Values.CreateSource;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Create.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class CreateTest {
  @Rule public final ExpectedException thrown = ExpectedException.none();
  @Rule public final TestPipeline p = TestPipeline.create();


  @Test
  @Category(ValidatesRunner.class)
  public void testCreate() {
    PCollection<String> output =
        p.apply(Create.of(LINES));

    PAssert.that(output)
        .containsInAnyOrder(LINES_ARRAY);
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCreateEmpty() {
    PCollection<String> output =
        p.apply(Create.empty(StringUtf8Coder.of()));

    PAssert.that(output)
        .containsInAnyOrder(NO_LINES_ARRAY);

    assertEquals(StringUtf8Coder.of(), output.getCoder());
    p.run();
  }

  @Test
  public void testCreateEmptyIterableRequiresCoder() {
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("determine a default Coder");
    thrown.expectMessage("Create.empty(Coder)");
    thrown.expectMessage("Create.empty(TypeDescriptor)");
    thrown.expectMessage("withCoder(Coder)");
    thrown.expectMessage("withType(TypeDescriptor)");
    p.apply(Create.of(Collections.emptyList()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCreateEmptyIterableWithCoder() {
    PCollection<Void> output =
        p.apply(Create.of(Collections.<Void>emptyList()).withCoder(VoidCoder.of()));

    assertEquals(VoidCoder.of(), output.getCoder());
    PAssert.that(output).empty();
    p.run();
  }

  static class Record implements Serializable {
  }

  static class Record2 extends Record {
  }

  private static class RecordCoder extends CustomCoder<Record> {
    @Override
    public void encode(Record value, OutputStream outStream, Context context)
        throws CoderException, IOException {}

    @Override
    public Record decode(InputStream inStream, Context context) throws CoderException, IOException {
      return null;
    }
  }

  @Test
  public void testPolymorphicType() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(
        Matchers.containsString("Unable to infer a coder"));

    // Create won't infer a default coder in this case.
    p.apply(Create.of(new Record(), new Record2()));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCreateWithNullsAndValues() throws Exception {
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

    static class UnserializableRecordCoder extends CustomCoder<UnserializableRecord> {
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
  @Category(ValidatesRunner.class)
  public void testCreateWithUnserializableElements() throws Exception {
    List<UnserializableRecord> elements =
        ImmutableList.of(
            new UnserializableRecord("foo"),
            new UnserializableRecord("bar"),
            new UnserializableRecord("baz"));
    Create.Values<UnserializableRecord> create =
        Create.of(elements).withCoder(new UnserializableRecord.UnserializableRecordCoder());

    PAssert.that(p.apply(create))
        .containsInAnyOrder(
            new UnserializableRecord("foo"),
            new UnserializableRecord("bar"),
            new UnserializableRecord("baz"));
    p.run();
  }

  private static class PrintTimestamps extends DoFn<String, String> {
    @ProcessElement
      public void processElement(ProcessContext c) {
      c.output(c.element() + ":" + c.timestamp().getMillis());
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCreateTimestamped() {
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
  @Category(ValidatesRunner.class)
  public void testCreateTimestampedEmpty() {
    PCollection<String> output = p
        .apply(Create.timestamped(new ArrayList<TimestampedValue<String>>())
            .withCoder(StringUtf8Coder.of()));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  public void testCreateTimestampedEmptyUnspecifiedCoder() {
    p.enableAbandonedNodeEnforcement(false);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("determine a default Coder");
    thrown.expectMessage("Create.empty(Coder)");
    thrown.expectMessage("Create.empty(TypeDescriptor)");
    thrown.expectMessage("withCoder(Coder)");
    thrown.expectMessage("withType(TypeDescriptor)");
    p.apply(Create.timestamped(new ArrayList<TimestampedValue<Object>>()));
  }

  @Test
  public void testCreateTimestampedPolymorphicType() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(
        Matchers.containsString("Unable to infer a coder"));

    // Create won't infer a default coder in this case.
    PCollection<Record> c = p.apply(Create.timestamped(
        TimestampedValue.of(new Record(), new Instant(0)),
        TimestampedValue.<Record>of(new Record2(), new Instant(0))));

    p.run();

    throw new RuntimeException("Coder: " + c.getCoder());
  }

  @Test
  public void testCreateTimestampedDefaultOutputCoderUsingCoder() throws Exception {
    Coder<Record> coder = new RecordCoder();
    PBegin pBegin = PBegin.in(p);
    Create.TimestampedValues<Record> values =
        Create.timestamped(
            TimestampedValue.of(new Record(), new Instant(0)),
            TimestampedValue.<Record>of(new Record2(), new Instant(0)))
            .withCoder(coder);
    Coder<Record> defaultCoder = values.getDefaultOutputCoder(pBegin);
    assertThat(defaultCoder, equalTo(coder));
  }

  @Test
  public void testCreateTimestampedDefaultOutputCoderUsingTypeDescriptor() throws Exception {
    Coder<Record> coder = new RecordCoder();
    p.getCoderRegistry().registerCoder(Record.class, coder);
    PBegin pBegin = PBegin.in(p);
    Create.TimestampedValues<Record> values =
        Create.timestamped(
            TimestampedValue.of(new Record(), new Instant(0)),
            TimestampedValue.<Record>of(new Record2(), new Instant(0)))
            .withType(new TypeDescriptor<Record>() {});
    Coder<Record> defaultCoder = values.getDefaultOutputCoder(pBegin);
    assertThat(defaultCoder, equalTo(coder));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCreateWithVoidType() throws Exception {
    PCollection<Void> output = p.apply(Create.of((Void) null, (Void) null));
    PAssert.that(output).containsInAnyOrder((Void) null, (Void) null);
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testCreateWithKVVoidType() throws Exception {
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
  public void testCreateDefaultOutputCoderUsingInference() throws Exception {
    Coder<Record> coder = new RecordCoder();
    p.getCoderRegistry().registerCoder(Record.class, coder);
    PBegin pBegin = PBegin.in(p);
    Create.Values<Record> values = Create.of(new Record(), new Record(), new Record());
    Coder<Record> defaultCoder = values.getDefaultOutputCoder(pBegin);
    assertThat(defaultCoder, equalTo(coder));
  }

  @Test
  public void testCreateDefaultOutputCoderUsingCoder() throws Exception {
    Coder<Record> coder = new RecordCoder();
    PBegin pBegin = PBegin.in(p);
    Create.Values<Record> values =
        Create.of(new Record(), new Record2()).withCoder(coder);
    Coder<Record> defaultCoder = values.getDefaultOutputCoder(pBegin);
    assertThat(defaultCoder, equalTo(coder));
  }

  @Test
  public void testCreateDefaultOutputCoderUsingTypeDescriptor() throws Exception {
    Coder<Record> coder = new RecordCoder();
    p.getCoderRegistry().registerCoder(Record.class, coder);
    PBegin pBegin = PBegin.in(p);
    Create.Values<Record> values =
        Create.of(new Record(), new Record2()).withType(new TypeDescriptor<Record>() {});
    Coder<Record> defaultCoder = values.getDefaultOutputCoder(pBegin);
    assertThat(defaultCoder, equalTo(coder));
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
  public void testSourceSplit() throws Exception {
    CreateSource<Integer> source =
        CreateSource.fromIterable(
            ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8), BigEndianIntegerCoder.of());
    PipelineOptions options = PipelineOptionsFactory.create();
    List<? extends BoundedSource<Integer>> splitSources = source.split(12, options);
    assertThat(splitSources, hasSize(3));
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splitSources, options);
  }

  @Test
  public void testSourceSplitVoid() throws Exception {
    CreateSource<Void> source =
        CreateSource.fromIterable(
            Lists.<Void>newArrayList(null, null, null, null, null), VoidCoder.of());
    PipelineOptions options = PipelineOptionsFactory.create();
    List<? extends BoundedSource<Void>> splitSources = source.split(3, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splitSources, options);
  }

  @Test
  public void testSourceSplitEmpty() throws Exception {
    CreateSource<Integer> source =
        CreateSource.fromIterable(ImmutableList.<Integer>of(), BigEndianIntegerCoder.of());
    PipelineOptions options = PipelineOptionsFactory.create();
    List<? extends BoundedSource<Integer>> splitSources = source.split(12, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splitSources, options);
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
