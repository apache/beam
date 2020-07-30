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

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList.toImmutableList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardCoders;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests that Java SDK coders standardized by the Fn API meet the common spec. */
@RunWith(Parameterized.class)
public class CommonCoderTest {
  private static final String STANDARD_CODERS_YAML_PATH =
      "/org/apache/beam/model/fnexecution/v1/standard_coders.yaml";

  private static final Map<String, Class<?>> coders =
      ImmutableMap.<String, Class<?>>builder()
          .put(getUrn(StandardCoders.Enum.BYTES), ByteCoder.class)
          .put(getUrn(StandardCoders.Enum.BOOL), BooleanCoder.class)
          .put(getUrn(StandardCoders.Enum.STRING_UTF8), StringUtf8Coder.class)
          .put(getUrn(StandardCoders.Enum.KV), KvCoder.class)
          .put(getUrn(StandardCoders.Enum.VARINT), VarLongCoder.class)
          .put(getUrn(StandardCoders.Enum.INTERVAL_WINDOW), IntervalWindowCoder.class)
          .put(getUrn(StandardCoders.Enum.ITERABLE), IterableCoder.class)
          .put(getUrn(StandardCoders.Enum.TIMER), Timer.Coder.class)
          .put(getUrn(StandardCoders.Enum.GLOBAL_WINDOW), GlobalWindow.Coder.class)
          .put(getUrn(StandardCoders.Enum.DOUBLE), DoubleCoder.class)
          .put(
              getUrn(StandardCoders.Enum.WINDOWED_VALUE),
              WindowedValue.FullWindowedValueCoder.class)
          .put(
              getUrn(StandardCoders.Enum.PARAM_WINDOWED_VALUE),
              WindowedValue.ParamWindowedValueCoder.class)
          .put(getUrn(StandardCoders.Enum.ROW), RowCoder.class)
          .build();

  @AutoValue
  abstract static class CommonCoder {
    abstract String getUrn();

    abstract List<CommonCoder> getComponents();

    @SuppressWarnings("mutable")
    abstract byte[] getPayload();

    abstract Boolean getNonDeterministic();

    @JsonCreator
    static CommonCoder create(
        @JsonProperty("urn") String urn,
        @JsonProperty("components") @Nullable List<CommonCoder> components,
        @JsonProperty("payload") @Nullable String payload,
        @JsonProperty("non_deterministic") @Nullable Boolean nonDeterministic) {
      return new AutoValue_CommonCoderTest_CommonCoder(
          checkNotNull(urn, "urn"),
          firstNonNull(components, Collections.emptyList()),
          firstNonNull(payload, "").getBytes(StandardCharsets.ISO_8859_1),
          firstNonNull(nonDeterministic, Boolean.FALSE));
    }
  }

  @AutoValue
  abstract static class CommonCoderTestSpec {
    abstract CommonCoder getCoder();

    abstract @Nullable Boolean getNested();

    abstract Map<String, Object> getExamples();

    @JsonCreator
    static CommonCoderTestSpec create(
        @JsonProperty("coder") CommonCoder coder,
        @JsonProperty("nested") @Nullable Boolean nested,
        @JsonProperty("examples") Map<String, Object> examples) {
      return new AutoValue_CommonCoderTest_CommonCoderTestSpec(coder, nested, examples);
    }
  }

  @AutoValue
  abstract static class OneCoderTestSpec {
    abstract CommonCoder getCoder();

    abstract boolean getNested();

    @SuppressWarnings("mutable")
    abstract byte[] getSerialized();

    abstract Object getValue();

    static OneCoderTestSpec create(
        CommonCoder coder, boolean nested, byte[] serialized, Object value) {
      return new AutoValue_CommonCoderTest_OneCoderTestSpec(coder, nested, serialized, value);
    }
  }

  private static List<OneCoderTestSpec> loadStandardCodersSuite() throws IOException {
    InputStream stream = CommonCoderTest.class.getResourceAsStream(STANDARD_CODERS_YAML_PATH);
    if (stream == null) {
      fail("Could not load standard coder specs as resource:" + STANDARD_CODERS_YAML_PATH);
    }

    // Would like to use the InputStream directly with Jackson, but Jackson does not seem to
    // support streams of multiple entities. Instead, read the entire YAML as a String and split
    // it up manually, passing each to Jackson.
    String specString = CharStreams.toString(new InputStreamReader(stream, StandardCharsets.UTF_8));
    Iterable<String> specs = Splitter.on("\n---\n").split(specString);
    List<OneCoderTestSpec> ret = new ArrayList<>();
    for (String spec : specs) {
      CommonCoderTestSpec coderTestSpec = parseSpec(spec);
      CommonCoder coder = coderTestSpec.getCoder();
      for (Map.Entry<String, Object> oneTestSpec : coderTestSpec.getExamples().entrySet()) {
        byte[] serialized = oneTestSpec.getKey().getBytes(StandardCharsets.ISO_8859_1);
        Object value = oneTestSpec.getValue();
        if (coderTestSpec.getNested() == null) {
          // Missing nested means both
          ret.add(OneCoderTestSpec.create(coder, true, serialized, value));
          ret.add(OneCoderTestSpec.create(coder, false, serialized, value));
        } else {
          ret.add(OneCoderTestSpec.create(coder, coderTestSpec.getNested(), serialized, value));
        }
      }
    }
    return ImmutableList.copyOf(ret);
  }

  @Parameters(name = "{1}")
  public static Iterable<Object[]> data() throws IOException {
    ImmutableList.Builder<Object[]> ret = ImmutableList.builder();
    for (OneCoderTestSpec test : loadStandardCodersSuite()) {
      // Some tools cannot handle Unicode in test names, so omit the problematic value field.
      String testname =
          MoreObjects.toStringHelper(OneCoderTestSpec.class)
              .add("coder", test.getCoder())
              .add("nested", test.getNested())
              .add("serialized", test.getSerialized())
              .toString();
      ret.add(new Object[] {test, testname});
    }
    return ret.build();
  }

  @Parameter(0)
  public OneCoderTestSpec testSpec;

  @Parameter(1)
  public String ignoredTestName;

  private static CommonCoderTestSpec parseSpec(String spec) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(spec, CommonCoderTestSpec.class);
  }

  private static void assertCoderIsKnown(CommonCoder coder) {
    assertThat("not a known coder", coders.keySet(), hasItem(coder.getUrn()));
    for (CommonCoder component : coder.getComponents()) {
      assertCoderIsKnown(component);
    }
  }

  /** Converts from JSON-auto-deserialized types into the proper Java types for the known coders. */
  private static Object convertValue(Object value, CommonCoder coderSpec, Coder coder) {
    String s = coderSpec.getUrn();
    if (s.equals(getUrn(StandardCoders.Enum.BYTES))) {
      return ((String) value).getBytes(StandardCharsets.ISO_8859_1);
    } else if (s.equals(getUrn(StandardCoders.Enum.BOOL))) {
      return value;
    } else if (s.equals(getUrn(StandardCoders.Enum.STRING_UTF8))) {
      return value;
    } else if (s.equals(getUrn(StandardCoders.Enum.KV))) {
      Coder keyCoder = ((KvCoder) coder).getKeyCoder();
      Coder valueCoder = ((KvCoder) coder).getValueCoder();
      Map<String, Object> kvMap = (Map<String, Object>) value;
      Object k = convertValue(kvMap.get("key"), coderSpec.getComponents().get(0), keyCoder);
      Object v = convertValue(kvMap.get("value"), coderSpec.getComponents().get(1), valueCoder);
      return KV.of(k, v);
    } else if (s.equals(getUrn(StandardCoders.Enum.VARINT))) {
      return ((Number) value).longValue();
    } else if (s.equals(getUrn(StandardCoders.Enum.TIMER))) {
      Map<String, Object> kvMap = (Map<String, Object>) value;
      Coder<?> keyCoder = ((Timer.Coder) coder).getValueCoder();
      Coder<? extends BoundedWindow> windowCoder = ((Timer.Coder) coder).getWindowCoder();
      List<BoundedWindow> windows = new ArrayList<>();
      for (Object window : (List<Object>) kvMap.get("windows")) {
        windows.add(
            (BoundedWindow) convertValue(window, coderSpec.getComponents().get(1), windowCoder));
      }
      if ((boolean) kvMap.get("clearBit")) {
        return Timer.cleared(
            convertValue(kvMap.get("userKey"), coderSpec.getComponents().get(0), keyCoder),
            (String) kvMap.get("dynamicTimerTag"),
            windows);
      }
      Map<String, Object> paneInfoMap = (Map<String, Object>) kvMap.get("pane");
      PaneInfo paneInfo =
          PaneInfo.createPane(
              (boolean) paneInfoMap.get("is_first"),
              (boolean) paneInfoMap.get("is_last"),
              PaneInfo.Timing.valueOf((String) paneInfoMap.get("timing")),
              (int) paneInfoMap.get("index"),
              (int) paneInfoMap.get("on_time_index"));
      return Timer.of(
          convertValue(kvMap.get("userKey"), coderSpec.getComponents().get(0), keyCoder),
          (String) kvMap.get("dynamicTimerTag"),
          windows,
          new Instant(((Number) kvMap.get("fireTimestamp")).longValue()),
          new Instant(((Number) kvMap.get("holdTimestamp")).longValue()),
          paneInfo);
    } else if (s.equals(getUrn(StandardCoders.Enum.INTERVAL_WINDOW))) {
      Map<String, Object> kvMap = (Map<String, Object>) value;
      Instant end = new Instant(((Number) kvMap.get("end")).longValue());
      Duration span = Duration.millis(((Number) kvMap.get("span")).longValue());
      return new IntervalWindow(end.minus(span), span);
    } else if (s.equals(getUrn(StandardCoders.Enum.ITERABLE))) {
      Coder elementCoder = ((IterableCoder) coder).getElemCoder();
      List<Object> elements = (List<Object>) value;
      List<Object> convertedElements = new ArrayList<>();
      for (Object element : elements) {
        convertedElements.add(
            convertValue(element, coderSpec.getComponents().get(0), elementCoder));
      }
      return convertedElements;
    } else if (s.equals(getUrn(StandardCoders.Enum.GLOBAL_WINDOW))) {
      return GlobalWindow.INSTANCE;
    } else if (s.equals(getUrn(StandardCoders.Enum.WINDOWED_VALUE))
        || s.equals(getUrn(StandardCoders.Enum.PARAM_WINDOWED_VALUE))) {
      Map<String, Object> kvMap = (Map<String, Object>) value;
      Coder valueCoder = ((WindowedValue.FullWindowedValueCoder) coder).getValueCoder();
      Coder windowCoder = ((WindowedValue.FullWindowedValueCoder) coder).getWindowCoder();
      Object windowValue =
          convertValue(kvMap.get("value"), coderSpec.getComponents().get(0), valueCoder);
      Instant timestamp = new Instant(((Number) kvMap.get("timestamp")).longValue());
      List<BoundedWindow> windows = new ArrayList<>();
      for (Object window : (List<Object>) kvMap.get("windows")) {
        windows.add(
            (BoundedWindow) convertValue(window, coderSpec.getComponents().get(1), windowCoder));
      }
      Map<String, Object> paneInfoMap = (Map<String, Object>) kvMap.get("pane");
      PaneInfo paneInfo =
          PaneInfo.createPane(
              (boolean) paneInfoMap.get("is_first"),
              (boolean) paneInfoMap.get("is_last"),
              PaneInfo.Timing.valueOf((String) paneInfoMap.get("timing")),
              (int) paneInfoMap.get("index"),
              (int) paneInfoMap.get("on_time_index"));
      return WindowedValue.of(windowValue, timestamp, windows, paneInfo);
    } else if (s.equals(getUrn(StandardCoders.Enum.DOUBLE))) {
      return Double.parseDouble((String) value);
    } else if (s.equals(getUrn(StandardCoders.Enum.ROW))) {
      Schema schema;
      try {
        schema =
            SchemaTranslation.schemaFromProto(SchemaApi.Schema.parseFrom(coderSpec.getPayload()));
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Failed to parse schema payload for row coder", e);
      }

      return parseField(value, Schema.FieldType.row(schema));
    } else {
      throw new IllegalStateException("Unknown coder URN: " + coderSpec.getUrn());
    }
  }

  private static Object parseField(Object value, Schema.FieldType fieldType) {
    if (value == null) return null;

    switch (fieldType.getTypeName()) {
      case BYTE:
        return ((Number) value).byteValue();
      case INT16:
        return ((Number) value).shortValue();
      case INT32:
        return ((Number) value).intValue();
      case INT64:
        return ((Number) value).longValue();
      case FLOAT:
        return Float.parseFloat((String) value);
      case DOUBLE:
        return Double.parseDouble((String) value);
      case STRING:
        return (String) value;
      case BOOLEAN:
        return (Boolean) value;
      case BYTES:
        // extract String as byte[]
        return ((String) value).getBytes(StandardCharsets.ISO_8859_1);
      case ARRAY:
        return ((List<Object>) value)
            .stream()
                .map((element) -> parseField(element, fieldType.getCollectionElementType()))
                .collect(toImmutableList());
      case MAP:
        Map<Object, Object> kvMap = new HashMap<>();
        ((Map<Object, Object>) value)
            .entrySet().stream()
                .forEach(
                    (entry) ->
                        kvMap.put(
                            parseField(entry.getKey(), fieldType.getMapKeyType()),
                            parseField(entry.getValue(), fieldType.getMapValueType())));
        return kvMap;
      case ROW:
        Map<String, Object> rowMap = (Map<String, Object>) value;
        Schema schema = fieldType.getRowSchema();
        Row.Builder row = Row.withSchema(schema);
        for (Schema.Field field : schema.getFields()) {
          Object element = rowMap.remove(field.getName());
          if (element != null) {
            element = parseField(element, field.getType());
          }
          row.addValue(element);
        }

        if (!rowMap.isEmpty()) {
          throw new IllegalArgumentException(
              "Value contains keys that are not in the schema: " + rowMap.keySet());
        }

        return row.build();
      default: // DECIMAL, DATETIME, LOGICAL_TYPE
        throw new IllegalArgumentException("Unsupported type name: " + fieldType.getTypeName());
    }
  }

  private static Coder<?> instantiateCoder(CommonCoder coder) {
    List<Coder<?>> components = new ArrayList<>();
    for (CommonCoder innerCoder : coder.getComponents()) {
      components.add(instantiateCoder(innerCoder));
    }
    Class<? extends Coder> coderType =
        ModelCoderRegistrar.BEAM_MODEL_CODER_URNS.inverse().get(coder.getUrn());
    checkNotNull(coderType, "Unknown coder URN: " + coder.getUrn());

    CoderTranslator<?> translator = ModelCoderRegistrar.BEAM_MODEL_CODERS.get(coderType);
    checkNotNull(
        translator, "No translator found for common coder class: " + coderType.getSimpleName());

    return translator.fromComponents(components, coder.getPayload(), new TranslationContext() {});
  }

  @Test
  public void executeSingleTest() throws IOException {
    assertCoderIsKnown(testSpec.getCoder());
    Coder coder = instantiateCoder(testSpec.getCoder());
    Object testValue = convertValue(testSpec.getValue(), testSpec.getCoder(), coder);
    Context context = testSpec.getNested() ? Context.NESTED : Context.OUTER;
    byte[] encoded = CoderUtils.encodeToByteArray(coder, testValue, context);
    Object decodedValue = CoderUtils.decodeFromByteArray(coder, testSpec.getSerialized(), context);

    if (!testSpec.getCoder().getNonDeterministic()) {
      assertThat(testSpec.toString(), encoded, equalTo(testSpec.getSerialized()));
    }
    verifyDecodedValue(testSpec.getCoder(), decodedValue, testValue);
  }

  private void verifyDecodedValue(CommonCoder coder, Object expectedValue, Object actualValue) {
    String s = coder.getUrn();
    if (s.equals(getUrn(StandardCoders.Enum.BYTES))) {
      assertThat(expectedValue, equalTo(actualValue));
    } else if (s.equals(getUrn(StandardCoders.Enum.BOOL))) {
      assertEquals(expectedValue, actualValue);
    } else if (s.equals(getUrn(StandardCoders.Enum.STRING_UTF8))) {
      assertEquals(expectedValue, actualValue);
    } else if (s.equals(getUrn(StandardCoders.Enum.KV))) {
      assertThat(actualValue, instanceOf(KV.class));
      verifyDecodedValue(
          coder.getComponents().get(0), ((KV) expectedValue).getKey(), ((KV) actualValue).getKey());
      verifyDecodedValue(
          coder.getComponents().get(0),
          ((KV) expectedValue).getValue(),
          ((KV) actualValue).getValue());

    } else if (s.equals(getUrn(StandardCoders.Enum.VARINT))) {
      assertEquals(expectedValue, actualValue);

    } else if (s.equals(getUrn(StandardCoders.Enum.INTERVAL_WINDOW))) {
      assertEquals(expectedValue, actualValue);

    } else if (s.equals(getUrn(StandardCoders.Enum.ITERABLE))) {
      assertThat(actualValue, instanceOf(Iterable.class));
      CommonCoder componentCoder = coder.getComponents().get(0);
      Iterator<Object> expectedValueIterator = ((Iterable<Object>) expectedValue).iterator();
      for (Object value : (Iterable<Object>) actualValue) {
        verifyDecodedValue(componentCoder, expectedValueIterator.next(), value);
      }
      assertFalse(expectedValueIterator.hasNext());

    } else if (s.equals(getUrn(StandardCoders.Enum.TIMER))) {
      assertEquals((Timer) expectedValue, (Timer) actualValue);

    } else if (s.equals(getUrn(StandardCoders.Enum.GLOBAL_WINDOW))) {
      assertEquals(expectedValue, actualValue);

    } else if (s.equals(getUrn(StandardCoders.Enum.WINDOWED_VALUE))) {
      assertEquals(expectedValue, actualValue);

    } else if (s.equals(getUrn(StandardCoders.Enum.PARAM_WINDOWED_VALUE))) {
      assertEquals(expectedValue, actualValue);

    } else if (s.equals(getUrn(StandardCoders.Enum.DOUBLE))) {

      assertEquals(expectedValue, actualValue);
    } else if (s.equals(getUrn(StandardCoders.Enum.ROW))) {
      assertEquals(expectedValue, actualValue);
    } else {
      throw new IllegalStateException("Unknown coder URN: " + coder.getUrn());
    }
  }

  /**
   * Utility for adding new entries to the common coder spec -- prints the serialized bytes of the
   * given value in the given context using JSON-escaped strings.
   */
  private static <T> String jsonByteString(Coder<T> coder, T value, Context context)
      throws CoderException {
    byte[] bytes = CoderUtils.encodeToByteArray(coder, value, context);
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
    try {
      return mapper.writeValueAsString(new String(bytes, StandardCharsets.ISO_8859_1));
    } catch (JsonProcessingException e) {
      throw new CoderException(String.format("Unable to encode %s with coder %s", value, coder), e);
    }
  }
}
