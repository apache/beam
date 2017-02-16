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
package org.apache.beam.sdk.coders;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests that Java SDK coders standardized by the Fn API meet the common spec.
 */
@RunWith(Parameterized.class)
public class CommonCoderTest {
  private static final String STANDARD_CODERS_YAML_PATH =
      "/org/apache/beam/fn/v1/standard_coders.yaml";

  private static final Map<String, Class<?>> coders = ImmutableMap.<String, Class<?>>builder()
      .put("urn:beam:coders:bytes:0.1", ByteCoder.class)
      .put("urn:beam:coders:kv:0.1", KvCoder.class)
      .put("urn:beam:coders:varint:0.1", VarLongCoder.class)
      .put("urn:beam:coders:intervalwindow:0.1", IntervalWindowCoder.class)
      .put("urn:beam:coders:stream:0.1", IterableCoder.class)
      .build();

  @AutoValue
  abstract static class CommonCoder {
    abstract String getUrn();
    abstract List<CommonCoder> getComponents();
    @JsonCreator
    static CommonCoder create(
        @JsonProperty("urn") String urn,
        @JsonProperty("components") @Nullable List<CommonCoder> components) {
      return new AutoValue_CommonCoderTest_CommonCoder(
          checkNotNull(urn, "urn"),
          firstNonNull(components, Collections.<CommonCoder>emptyList()));
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
      fail("null stream");
    }

    // Would like to use the InputStream directly with Jackson, but Jackson does not seem to
    // support streams of multiple entities. Instead, read the entire YAML as a String and split
    // it up manually, passing each to Jackson.
    String specString = CharStreams.toString(new InputStreamReader(stream));
    String[] specs = specString.split("\n---\n");
    List<OneCoderTestSpec> ret = new LinkedList<>();
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
      String testname = MoreObjects.toStringHelper(OneCoderTestSpec.class)
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
    switch (coderSpec.getUrn()) {
      case "urn:beam:coders:bytes:0.1": {
        return ((String) value).getBytes(StandardCharsets.ISO_8859_1);
      }
      case "urn:beam:coders:kv:0.1": {
        Coder keyCoder = ((KvCoder) coder).getKeyCoder();
        Coder valueCoder = ((KvCoder) coder).getValueCoder();
        Map<String, Object> kvMap = (Map<String, Object>) value;
        Object k = convertValue(kvMap.get("key"), coderSpec.getComponents().get(0), keyCoder);
        Object v = convertValue(kvMap.get("value"), coderSpec.getComponents().get(1), valueCoder);
        return KV.of(k, v);
      }
      case "urn:beam:coders:varint:0.1":
        return ((Number) value).longValue();
      case "urn:beam:coders:intervalwindow:0.1": {
        Map<String, Object> kvMap = (Map<String, Object>) value;
        Instant end = new Instant(((Number) kvMap.get("end")).longValue());
        Duration span = Duration.millis(((Number) kvMap.get("span")).longValue());
        return new IntervalWindow(end.minus(span), span);
      }
      case "urn:beam:coders:stream:0.1":
        Coder elementCoder = ((IterableCoder) coder).getElemCoder();
        List<Object> elements = (List<Object>) value;
        List<Object> convertedElements = new LinkedList<>();
        for (Object element : elements) {
          convertedElements.add(
              convertValue(element, coderSpec.getComponents().get(0), elementCoder));
        }
        return convertedElements;
      default:
        throw new IllegalStateException("Unknown coder URN: " + coderSpec.getUrn());
    }
  }

  private static Coder<?> instantiateCoder(CommonCoder coder) {
    List<Coder<?>> components = new LinkedList<>();
    for (CommonCoder innerCoder : coder.getComponents()) {
      components.add(instantiateCoder(innerCoder));
    }
    switch (coder.getUrn()) {
      case "urn:beam:coders:bytes:0.1":
        return ByteArrayCoder.of();
      case "urn:beam:coders:kv:0.1":
        return KvCoder.of(components.get(0), components.get(1));
      case "urn:beam:coders:varint:0.1":
        return VarLongCoder.of();
      case "urn:beam:coders:intervalwindow:0.1":
        return IntervalWindowCoder.of();
      case "urn:beam:coders:stream:0.1":
        return IterableCoder.of(components.get(0));
      default:
        throw new IllegalStateException("Unknown coder URN: " + coder.getUrn());
    }
  }

  @Test
  public void executeSingleTest() throws IOException {
    assertCoderIsKnown(testSpec.getCoder());
    Coder coder = instantiateCoder(testSpec.getCoder());
    Object testValue = convertValue(testSpec.getValue(), testSpec.getCoder(), coder);
    Context context = testSpec.getNested() ? Context.NESTED : Context.OUTER;
    byte[] encoded = CoderUtils.encodeToByteArray(coder, testValue, context);
    assertThat(testSpec.toString(), encoded, equalTo(testSpec.getSerialized()));
  }

  /**
   * Utility for adding new entries to the common coder spec -- prints the serialized bytes of
   * the given value in the given context using JSON-escaped strings.
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
