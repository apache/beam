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
package org.apache.beam.sdk.extensions.jackson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;

/** Test Jackson transforms {@link ParseJsons} and {@link AsJsons}. */
public class JacksonTransformsTest implements Serializable {
  private static final List<String> VALID_JSONS =
      Arrays.asList("{\"myString\":\"abc\",\"myInt\":3}", "{\"myString\":\"def\",\"myInt\":4}");

  private static final List<String> INVALID_JSONS =
      Arrays.asList("{myString:\"abc\",\"myInt\":3,\"other\":1}", "{", "");

  private static final List<String> EMPTY_JSONS = Arrays.asList("{}", "{}");

  private static final List<String> EXTRA_PROPERTIES_JSONS =
      Arrays.asList(
          "{\"myString\":\"abc\",\"myInt\":3,\"other\":1}", "{\"myString\":\"def\",\"myInt\":4}");

  private static final List<MyPojo> POJOS =
      Arrays.asList(new MyPojo("abc", 3), new MyPojo("def", 4));

  private static final List<MyInvalidPojo> INVALID_POJOS =
      Arrays.asList(new MyInvalidPojo("aaa", 5), new MyInvalidPojo("bbb", 6));

  private static final List<MyEmptyBean> EMPTY_BEANS =
      Arrays.asList(new MyEmptyBean("abc", 3), new MyEmptyBean("def", 4));

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void parseValidJsons() {
    PCollection<MyPojo> output =
        pipeline
            .apply(Create.of(VALID_JSONS))
            .apply(ParseJsons.of(MyPojo.class))
            .setCoder(SerializableCoder.of(MyPojo.class));

    PAssert.that(output).containsInAnyOrder(POJOS);

    pipeline.run();
  }

  @Test(expected = Pipeline.PipelineExecutionException.class)
  public void failParsingInvalidJsons() {
    PCollection<MyPojo> output =
        pipeline
            .apply(Create.of(Iterables.concat(VALID_JSONS, INVALID_JSONS)))
            .apply(ParseJsons.of(MyPojo.class))
            .setCoder(SerializableCoder.of(MyPojo.class));

    PAssert.that(output).containsInAnyOrder(POJOS);

    pipeline.run();
  }

  @Test
  public void testParsingInvalidJsonsWithFailuresDefaultHandler() {
    WithFailures.Result<PCollection<MyPojo>, KV<String, Map<String, String>>> result =
        pipeline
            .apply(Create.of(Iterables.concat(VALID_JSONS, INVALID_JSONS)))
            .apply(ParseJsons.of(MyPojo.class).exceptionsVia());

    result.output().setCoder(SerializableCoder.of(MyPojo.class));

    PAssert.that(result.output()).containsInAnyOrder(POJOS);
    assertParsingWithErrorMapHandler(result);

    pipeline.run();
  }

  @Test
  public void testParsingInvalidJsonsWithFailuresAsMap() {
    WithFailures.Result<PCollection<MyPojo>, KV<String, Map<String, String>>> result =
        pipeline
            .apply(Create.of(Iterables.concat(VALID_JSONS, INVALID_JSONS)))
            .apply(
                ParseJsons.of(MyPojo.class)
                    .exceptionsVia(new WithFailures.ExceptionAsMapHandler<String>() {}));

    result.output().setCoder(SerializableCoder.of(MyPojo.class));

    PAssert.that(result.output()).containsInAnyOrder(POJOS);
    assertParsingWithErrorMapHandler(result);

    pipeline.run();
  }

  @Test
  public void testParsingInvalidJsonsWithFailuresSimpleFunction() {
    WithFailures.Result<PCollection<MyPojo>, KV<String, String>> result =
        pipeline
            .apply(Create.of(Iterables.concat(VALID_JSONS, INVALID_JSONS)))
            .apply(
                ParseJsons.of(MyPojo.class)
                    .exceptionsVia(
                        new SimpleFunction<
                            WithFailures.ExceptionElement<String>, KV<String, String>>() {
                          @Override
                          public KV<String, String> apply(
                              WithFailures.ExceptionElement<String> failure) {
                            return KV.of(
                                failure.element(),
                                failure.exception().getClass().getCanonicalName());
                          }
                        }));
    result.output().setCoder(SerializableCoder.of(MyPojo.class));

    PAssert.that(result.output()).containsInAnyOrder(POJOS);
    assertParsingWithErrorFunctionHandler(result);

    pipeline.run();
  }

  @Test
  public void testParsingInvalidJsonsWithFailuresLambda() {
    WithFailures.Result<PCollection<MyPojo>, KV<String, String>> result =
        pipeline
            .apply(Create.of(Iterables.concat(VALID_JSONS, INVALID_JSONS)))
            .apply(
                ParseJsons.of(MyPojo.class)
                    .exceptionsInto(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                    .exceptionsVia(
                        f -> KV.of(f.element(), f.exception().getClass().getCanonicalName())));
    result.output().setCoder(SerializableCoder.of(MyPojo.class));

    PAssert.that(result.output()).containsInAnyOrder(POJOS);
    assertParsingWithErrorFunctionHandler(result);

    pipeline.run();
  }

  @Test(expected = Pipeline.PipelineExecutionException.class)
  public void failParsingWithoutCustomMapper() {
    PCollection<MyPojo> output =
        pipeline
            .apply(Create.of(EXTRA_PROPERTIES_JSONS))
            .apply(ParseJsons.of(MyPojo.class))
            .setCoder(SerializableCoder.of(MyPojo.class));

    PAssert.that(output).empty();

    pipeline.run();
  }

  @Test
  public void parseUsingCustomMapper() {
    ObjectMapper customMapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    PCollection<MyPojo> output =
        pipeline
            .apply(Create.of(EXTRA_PROPERTIES_JSONS))
            .apply(ParseJsons.of(MyPojo.class).withMapper(customMapper))
            .setCoder(SerializableCoder.of(MyPojo.class));

    PAssert.that(output).containsInAnyOrder(POJOS);

    pipeline.run();
  }

  @Test
  public void writeValidObjects() {
    PCollection<String> output =
        pipeline
            .apply(Create.of(POJOS))
            .apply(AsJsons.of(MyPojo.class))
            .setCoder(StringUtf8Coder.of());

    PAssert.that(output).containsInAnyOrder(VALID_JSONS);

    pipeline.run();
  }

  @Test(expected = Pipeline.PipelineExecutionException.class)
  public void failWritingWithoutCustomMapper() {
    pipeline
        .apply(Create.of(EMPTY_BEANS))
        .apply(AsJsons.of(MyEmptyBean.class))
        .setCoder(StringUtf8Coder.of());

    pipeline.run();
  }

  @Test
  public void writeUsingCustomMapper() {
    ObjectMapper customMapper =
        new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    PCollection<String> output =
        pipeline
            .apply(Create.of(EMPTY_BEANS))
            .apply(AsJsons.of(MyEmptyBean.class).withMapper(customMapper))
            .setCoder(StringUtf8Coder.of());

    PAssert.that(output).containsInAnyOrder(EMPTY_JSONS);

    pipeline.run();
  }

  @Test
  public void testWritingInvalidJsonsWithFailuresDefaultHandler() {
    WithFailures.Result<PCollection<String>, KV<MyPojo, Map<String, String>>> result =
        pipeline
            .apply(
                Create.of(Iterables.concat(POJOS, INVALID_POJOS))
                    .withCoder(SerializableCoder.of(MyPojo.class)))
            .apply(AsJsons.of(MyPojo.class).exceptionsVia());

    result.output().setCoder(StringUtf8Coder.of());

    result
        .failures()
        .setCoder(
            KvCoder.of(
                SerializableCoder.of(MyPojo.class),
                MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    PAssert.that(result.output()).containsInAnyOrder(VALID_JSONS);
    assertWritingWithErrorMapHandler(result);

    pipeline.run();
  }

  @Test
  public void testWritingInvalidJsonsWithFailuresAsMap() {
    WithFailures.Result<PCollection<String>, KV<MyPojo, Map<String, String>>> result =
        pipeline
            .apply(
                Create.of(Iterables.concat(POJOS, INVALID_POJOS))
                    .withCoder(SerializableCoder.of(MyPojo.class)))
            .apply(
                AsJsons.of(MyPojo.class)
                    .exceptionsVia(new WithFailures.ExceptionAsMapHandler<MyPojo>() {}));

    result.output().setCoder(StringUtf8Coder.of());

    PAssert.that(result.output()).containsInAnyOrder(VALID_JSONS);
    assertWritingWithErrorMapHandler(result);

    pipeline.run();
  }

  @Test
  public void testWritingInvalidJsonsWithFailuresSimpleFunction() {
    WithFailures.Result<PCollection<String>, KV<MyPojo, String>> result =
        pipeline
            .apply(
                Create.of(Iterables.concat(POJOS, INVALID_POJOS))
                    .withCoder(SerializableCoder.of(MyPojo.class)))
            .apply(
                AsJsons.of(MyPojo.class)
                    .exceptionsVia(
                        new SimpleFunction<
                            WithFailures.ExceptionElement<MyPojo>, KV<MyPojo, String>>() {
                          @Override
                          public KV<MyPojo, String> apply(
                              WithFailures.ExceptionElement<MyPojo> failure) {
                            return KV.of(
                                failure.element(),
                                failure.exception().getClass().getCanonicalName());
                          }
                        }));
    result.output().setCoder(StringUtf8Coder.of());

    PAssert.that(result.output()).containsInAnyOrder(VALID_JSONS);
    assertWritingWithErrorFunctionHandler(result);

    pipeline.run();
  }

  @Test
  public void testWritingInvalidJsonsWithFailuresLambda() {
    WithFailures.Result<PCollection<String>, KV<MyPojo, String>> result =
        pipeline
            .apply(
                Create.of(Iterables.concat(POJOS, INVALID_POJOS))
                    .withCoder(SerializableCoder.of(MyPojo.class)))
            .apply(
                AsJsons.of(MyPojo.class)
                    .exceptionsInto(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(MyPojo.class), TypeDescriptors.strings()))
                    .exceptionsVia(
                        f -> KV.of(f.element(), f.exception().getClass().getCanonicalName())));
    result.output().setCoder(StringUtf8Coder.of());

    PAssert.that(result.output()).containsInAnyOrder(VALID_JSONS);
    assertWritingWithErrorFunctionHandler(result);

    pipeline.run();
  }

  /** Pojo for tests. */
  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class MyPojo implements Serializable {
    private String myString;
    private int myInt;

    public MyPojo() {}

    public MyPojo(String myString, int myInt) {
      this.myString = myString;
      this.myInt = myInt;
    }

    public String getMyString() {
      return myString;
    }

    public void setMyString(String myString) {
      this.myString = myString;
    }

    public int getMyInt() {
      return myInt;
    }

    public void setMyInt(int myInt) {
      this.myInt = myInt;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof MyPojo)) {
        return false;
      }

      MyPojo myPojo = (MyPojo) o;

      return myInt == myPojo.myInt
          && (myString != null ? myString.equals(myPojo.myString) : myPojo.myString == null);
    }

    @Override
    public int hashCode() {
      int result = myString != null ? myString.hashCode() : 0;
      result = 31 * result + myInt;
      return result;
    }
  }

  /** Pojo for tests. */
  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class MyEmptyBean implements Serializable {
    private String myString;
    private int myInt;

    public MyEmptyBean(String myString, int myInt) {
      this.myString = myString;
      this.myInt = myInt;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      MyEmptyBean that = (MyEmptyBean) o;

      if (myInt != that.myInt) {
        return false;
      }
      return myString != null ? myString.equals(that.myString) : that.myString == null;
    }

    @Override
    public int hashCode() {
      int result = myString != null ? myString.hashCode() : 0;
      result = 31 * result + myInt;
      return result;
    }
  }

  /** Pojo for tests. */
  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class MyInvalidPojo extends MyPojo {
    public MyInvalidPojo(String myString, int myInt) {
      super(myString, myInt);
    }

    @Override
    public String getMyString() {
      throw new RuntimeException("Unknown error!");
    }
  }

  private void assertParsingWithErrorMapHandler(
      WithFailures.Result<PCollection<MyPojo>, KV<String, Map<String, String>>> result) {
    PAssert.that(result.failures())
        .satisfies(
            kv -> {
              for (KV<String, Map<String, String>> entry : kv) {
                if (entry.getKey().equals(INVALID_JSONS.get(0))) {
                  assertEquals(
                      "com.fasterxml.jackson.core.JsonParseException",
                      entry.getValue().get("className"));
                } else if (entry.getKey().equals(INVALID_JSONS.get(1))) {
                  assertEquals(
                      "com.fasterxml.jackson.core.io.JsonEOFException",
                      entry.getValue().get("className"));
                } else if (entry.getKey().equals(INVALID_JSONS.get(2))) {
                  assertEquals(
                      "com.fasterxml.jackson.databind.exc.MismatchedInputException",
                      entry.getValue().get("className"));
                } else {
                  throw new AssertionError(
                      "Unexpected key is found in failures result: \"" + entry.getKey() + "\"");
                }
                assertThat(entry.getValue().entrySet(), hasSize(3));
                assertThat(entry.getValue(), hasKey("stackTrace"));
                assertThat(entry.getValue(), hasKey("message"));
              }

              return null;
            });
  }

  private void assertParsingWithErrorFunctionHandler(
      WithFailures.Result<PCollection<MyPojo>, KV<String, String>> result) {
    PAssert.that(result.failures())
        .containsInAnyOrder(
            KV.of(INVALID_JSONS.get(0), "com.fasterxml.jackson.core.JsonParseException"),
            KV.of(INVALID_JSONS.get(1), "com.fasterxml.jackson.core.io.JsonEOFException"),
            KV.of(
                INVALID_JSONS.get(2),
                "com.fasterxml.jackson.databind.exc.MismatchedInputException"));
  }

  private void assertWritingWithErrorMapHandler(
      WithFailures.Result<PCollection<String>, KV<MyPojo, Map<String, String>>> result) {
    PAssert.that(result.failures())
        .satisfies(
            kv -> {
              for (KV<MyPojo, Map<String, String>> entry : kv) {
                assertThat(entry.getValue().entrySet(), hasSize(3));
                assertThat(entry.getValue(), hasKey("stackTrace"));
                assertThat(entry.getValue(), hasKey("message"));
                assertEquals(
                    "com.fasterxml.jackson.databind.JsonMappingException",
                    entry.getValue().get("className"));
              }
              return null;
            });
  }

  private void assertWritingWithErrorFunctionHandler(
      WithFailures.Result<PCollection<String>, KV<MyPojo, String>> result) {
    PAssert.that(result.failures())
        .containsInAnyOrder(
            KV.of(INVALID_POJOS.get(0), "com.fasterxml.jackson.databind.JsonMappingException"),
            KV.of(INVALID_POJOS.get(1), "com.fasterxml.jackson.databind.JsonMappingException"));
  }
}
