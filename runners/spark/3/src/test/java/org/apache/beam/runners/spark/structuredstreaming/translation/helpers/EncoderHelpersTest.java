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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.collectionEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.encoderFor;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.kvEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.mapEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.oneOfEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.windowedValueEncoder;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.tuple;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates.notNull;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.BigEndianShortCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.FloatCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import scala.Tuple2;

/** Test of the wrapping of Beam Coders as Spark ExpressionEncoders. */
@RunWith(JUnit4.class)
public class EncoderHelpersTest {
  @ClassRule public static SparkSessionRule sessionRule = new SparkSessionRule("local[1]");

  private static final Encoder<GlobalWindow> windowEnc =
      EncoderHelpers.encoderOf(GlobalWindow.class);

  private static final Map<Coder<?>, List<?>> BASIC_CASES =
      ImmutableMap.<Coder<?>, List<?>>builder()
          .put(BooleanCoder.of(), asList(true, false, null))
          .put(ByteCoder.of(), asList((byte) 1, null))
          .put(BigEndianShortCoder.of(), asList((short) 1, null))
          .put(BigEndianIntegerCoder.of(), asList(1, 2, 3, null))
          .put(VarIntCoder.of(), asList(1, 2, 3, null))
          .put(BigEndianLongCoder.of(), asList(1L, 2L, 3L, null))
          .put(VarLongCoder.of(), asList(1L, 2L, 3L, null))
          .put(FloatCoder.of(), asList((float) 1.0, (float) 2.0, null))
          .put(DoubleCoder.of(), asList(1.0, 2.0, null))
          .put(StringUtf8Coder.of(), asList("1", "2", null))
          .put(BigDecimalCoder.of(), asList(bigDecimalOf(1L), bigDecimalOf(2L), null))
          .put(InstantCoder.of(), asList(Instant.ofEpochMilli(1), null))
          .build();

  private <T> Dataset<T> createDataset(List<T> data, Encoder<T> encoder) {
    Dataset<T> ds = sessionRule.getSession().createDataset(data, encoder);
    ds.printSchema();
    return ds;
  }

  @Test
  public void testBeamEncoderMappings() {
    BASIC_CASES.forEach(
        (coder, data) -> {
          Encoder<?> encoder = encoderFor(coder);
          serializeAndDeserialize(data.get(0), (Encoder) encoder);
          Dataset<?> dataset = createDataset(data, (Encoder) encoder);
          assertThat(dataset.collect(), equalTo(data.toArray()));
        });
  }

  @Test
  public void testBeamEncoderOfPrivateType() {
    // Verify concrete types are not used in coder generation.
    // In case of private types this would cause an IllegalAccessError.
    List<PrivateString> data = asList(new PrivateString("1"), new PrivateString("2"));
    Dataset<PrivateString> dataset = createDataset(data, encoderFor(PrivateString.CODER));
    assertThat(dataset.collect(), equalTo(data.toArray()));
  }

  @Test
  public void testBeamWindowedValueEncoderMappings() {
    BASIC_CASES.forEach(
        (coder, data) -> {
          List<WindowedValue<?>> windowed =
              Lists.transform(data, WindowedValue::valueInGlobalWindow);

          Encoder<?> encoder = windowedValueEncoder(encoderFor(coder), windowEnc);
          serializeAndDeserialize(windowed.get(0), (Encoder) encoder);

          Dataset<?> dataset = createDataset(windowed, (Encoder) encoder);
          assertThat(dataset.collect(), equalTo(windowed.toArray()));
        });
  }

  @Test
  public void testCollectionEncoder() {
    BASIC_CASES.forEach(
        (coder, data) -> {
          Encoder<? extends Collection<?>> encoder = collectionEncoder(encoderFor(coder), true);
          Collection<?> collection = Collections.unmodifiableCollection(data);

          Dataset<Collection<?>> dataset = createDataset(asList(collection), (Encoder) encoder);
          assertThat(dataset.head(), equalTo(data));
        });
  }

  private void testMapEncoder(Class<?> cls, Function<Map<?, ?>, Map<?, ?>> decorator) {
    BASIC_CASES.forEach(
        (coder, data) -> {
          Encoder<?> enc = encoderFor(coder);
          Encoder<Map<?, ?>> mapEncoder = mapEncoder(enc, enc, (Class) cls);
          Map<?, ?> map =
              decorator.apply(
                  data.stream().filter(notNull()).collect(toMap(identity(), identity())));

          Dataset<Map<?, ?>> dataset = createDataset(asList(map), mapEncoder);
          Map<?, ?> head = dataset.head();
          assertThat(head, equalTo(map));
          assertThat(head, instanceOf(cls));
        });
  }

  @Test
  public void testMapEncoder() {
    testMapEncoder(Map.class, identity());
  }

  @Test
  public void testHashMapEncoder() {
    testMapEncoder(HashMap.class, identity());
  }

  @Test
  public void testTreeMapEncoder() {
    testMapEncoder(TreeMap.class, TreeMap::new);
  }

  @Test
  public void testBeamBinaryEncoder() {
    List<List<String>> data = asList(asList("a1", "a2", "a3"), asList("b1", "b2"), asList("c1"));

    Encoder<List<String>> encoder = encoderFor(ListCoder.of(StringUtf8Coder.of()));
    serializeAndDeserialize(data.get(0), encoder);

    Dataset<List<String>> dataset = createDataset(data, encoder);
    assertThat(dataset.collect(), equalTo(data.toArray()));
  }

  @Test
  public void testEncoderForKVCoder() {
    List<KV<Integer, String>> data =
        asList(KV.of(1, "value1"), KV.of(null, "value2"), KV.of(3, null));

    Encoder<KV<Integer, String>> encoder =
        kvEncoder(encoderFor(VarIntCoder.of()), encoderFor(StringUtf8Coder.of()));
    serializeAndDeserialize(data.get(0), encoder);

    Dataset<KV<Integer, String>> dataset = createDataset(data, encoder);

    StructType kvSchema =
        createStructType(
            new StructField[] {
              createStructField("key", IntegerType, true),
              createStructField("value", StringType, true)
            });

    assertThat(dataset.schema(), equalTo(kvSchema));
    assertThat(dataset.collectAsList(), equalTo(data));
  }

  @Test
  public void testOneOffEncoder() {
    List<Coder<?>> coders = ImmutableList.copyOf(BASIC_CASES.keySet());
    List<Encoder<?>> encoders = coders.stream().map(EncoderHelpers::encoderFor).collect(toList());

    // build oneOf tuples of type index and corresponding value
    List<Tuple2<Integer, ?>> data =
        BASIC_CASES.entrySet().stream()
            .map(e -> tuple(coders.indexOf(e.getKey()), (Object) e.getValue().get(0)))
            .collect(toList());

    // dataset is a sparse dataset with only one column set per row
    Dataset<Tuple2<Integer, ?>> dataset = createDataset(data, oneOfEncoder((List) encoders));
    assertThat(dataset.collectAsList(), equalTo(data));
  }

  // fix scale/precision to system default to compare using equals
  private static BigDecimal bigDecimalOf(long l) {
    DecimalType type = DecimalType.SYSTEM_DEFAULT();
    return new BigDecimal(l, new MathContext(type.precision())).setScale(type.scale());
  }

  // test and explicit serialization roundtrip
  private static <T> void serializeAndDeserialize(T data, Encoder<T> enc) {
    ExpressionEncoder<T> bound = (ExpressionEncoder<T>) enc;
    bound =
        bound.resolveAndBind(bound.resolveAndBind$default$1(), bound.resolveAndBind$default$2());

    InternalRow row = bound.createSerializer().apply(data);
    T deserialized = bound.createDeserializer().apply(row);

    assertThat(deserialized, equalTo(data));
  }

  private static class PrivateString {
    private static final Coder<PrivateString> CODER =
        DelegateCoder.of(
            StringUtf8Coder.of(),
            str -> str.string,
            PrivateString::new,
            new TypeDescriptor<PrivateString>() {});

    private final String string;

    public PrivateString(String string) {
      this.string = string;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PrivateString that = (PrivateString) o;
      return Objects.equals(string, that.string);
    }

    @Override
    public int hashCode() {
      return Objects.hash(string);
    }
  }
}
