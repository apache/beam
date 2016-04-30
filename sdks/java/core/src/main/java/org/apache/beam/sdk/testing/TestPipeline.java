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
package org.apache.beam.sdk.testing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.TestCredential;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleSerializers;

import org.hamcrest.Matcher;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

/**
 * A creator of test pipelines that can be used inside of tests that can be
 * configured to run locally or against a remote pipeline runner.
 *
 * <p>It is recommended to tag hand-selected tests for this purpose using the
 * {@link RunnableOnService} {@link Category} annotation, as each test run against a pipeline runner
 * will utilize resources of that pipeline runner.
 *
 * <p>In order to run tests on a pipeline runner, the following conditions must be met:
 * <ul>
 *   <li>System property "beamTestPipelineOptions" must contain a JSON delimited list of pipeline
 *   options. For example:
 *   <pre>{@code [
 *     "--runner=org.apache.beam.runners.dataflow.testing.TestDataflowPipelineRunner",
 *     "--project=mygcpproject",
 *     "--stagingLocation=gs://mygcsbucket/path"
 *     ]}</pre>
 *     Note that the set of pipeline options required is pipeline runner specific.
 *   </li>
 *   <li>Jars containing the SDK and test classes must be available on the classpath.</li>
 * </ul>
 *
 * <p>Use {@link PAssert} for tests, as it integrates with this test harness in both direct and
 * remote execution modes. For example:
 * <pre>{@code
 * Pipeline p = TestPipeline.create();
 * PCollection<Integer> output = ...
 *
 * PAssert.that(output)
 *     .containsInAnyOrder(1, 2, 3, 4);
 * p.run();
 * }</pre>
 *
 * <p>For pipeline runners, it is required that they must throw an {@link AssertionError}
 * containing the message from the {@link PAssert} that failed.
 */
public class TestPipeline extends Pipeline {
  private static final String PROPERTY_BEAM_TEST_PIPELINE_OPTIONS = "beamTestPipelineOptions";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new IntegrationMatcherStore());
  }

  /**
   * Creates and returns a new test pipeline.
   *
   * <p>Use {@link PAssert} to add tests, then call
   * {@link Pipeline#run} to execute the pipeline and check the tests.
   */
  public static TestPipeline create() {
    return fromOptions(testingPipelineOptions());
  }

  public static TestPipeline fromOptions(PipelineOptions options) {
    return new TestPipeline(PipelineRunner.fromOptions(options), options);
  }

  private TestPipeline(PipelineRunner<? extends PipelineResult> runner, PipelineOptions options) {
    super(runner, options);
  }

  /**
   * Runs this {@link TestPipeline}, unwrapping any {@code AssertionError}
   * that is raised during testing.
   */
  @Override
  public PipelineResult run() {
    try {
      return super.run();
    } catch (RuntimeException exc) {
      Throwable cause = exc.getCause();
      if (cause instanceof AssertionError) {
        throw (AssertionError) cause;
      } else {
        throw exc;
      }
    }
  }

  @Override
  public String toString() {
    return "TestPipeline#" + getOptions().as(ApplicationNameOptions.class).getAppName();
  }

  /**
   * Creates {@link PipelineOptions} for testing.
   */
  public static PipelineOptions testingPipelineOptions() {
    try {
      @Nullable String beamTestPipelineOptions =
          System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS);

      PipelineOptions options =
          Strings.isNullOrEmpty(beamTestPipelineOptions)
              ? PipelineOptionsFactory.create()
              : PipelineOptionsFactory.fromArgs(
                      MAPPER.readValue(
                          System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS), String[].class))
                  .as(TestPipelineOptions.class);

      options.as(ApplicationNameOptions.class).setAppName(getAppName());
      // If no options were specified, use a test credential object on all pipelines.
      if (Strings.isNullOrEmpty(beamTestPipelineOptions)) {
        options.as(GcpOptions.class).setGcpCredential(new TestCredential());
      }
      options.setStableUniqueNames(CheckEnabled.ERROR);
      return options;
    } catch (IOException e) {
      throw new RuntimeException("Unable to instantiate test options from system property "
          + PROPERTY_BEAM_TEST_PIPELINE_OPTIONS + ":"
          + System.getProperty(PROPERTY_BEAM_TEST_PIPELINE_OPTIONS), e);
    }
  }


  public static String[] convertToArgs(PipelineOptions options) {
    try {
      Map<String, Object> stringOpts = (Map<String, Object>) MAPPER.readValue(
          MAPPER.writeValueAsBytes(options), Map.class).get("options");

      ArrayList<String> optArrayList = new ArrayList<>();
      for (Map.Entry<String, Object> entry : stringOpts.entrySet()) {
        optArrayList.add("--" + entry.getKey() + "=" + entry.getValue());
      }
      return optArrayList.toArray(new String[optArrayList.size()]);
    } catch (Exception e) {
      return null;
    }
  }

  /** Returns the class + method name of the test, or a default name. */
  private static String getAppName() {
    Optional<StackTraceElement> stackTraceElement = findCallersStackTrace();
    if (stackTraceElement.isPresent()) {
      String methodName = stackTraceElement.get().getMethodName();
      String className = stackTraceElement.get().getClassName();
      if (className.contains(".")) {
        className = className.substring(className.lastIndexOf(".") + 1);
      }
      return className + "-" + methodName;
    }
    return "UnitTest";
  }

  /** Returns the {@link StackTraceElement} of the calling class. */
  private static Optional<StackTraceElement> findCallersStackTrace() {
    Iterator<StackTraceElement> elements =
        Iterators.forArray(Thread.currentThread().getStackTrace());
    // First find the TestPipeline class in the stack trace.
    while (elements.hasNext()) {
      StackTraceElement next = elements.next();
      if (TestPipeline.class.getName().equals(next.getClassName())) {
        break;
      }
    }
    // Then find the first instance after that is not the TestPipeline
    Optional<StackTraceElement> firstInstanceAfterTestPipeline = Optional.absent();
    while (elements.hasNext()) {
      StackTraceElement next = elements.next();
      if (!TestPipeline.class.getName().equals(next.getClassName())) {
        if (!firstInstanceAfterTestPipeline.isPresent()) {
          firstInstanceAfterTestPipeline = Optional.of(next);
        }
        try {
          Class<?> nextClass = Class.forName(next.getClassName());
          for (Method method : nextClass.getMethods()) {
            if (method.getName().equals(next.getMethodName())) {
              if (method.isAnnotationPresent(org.junit.Test.class)) {
                return Optional.of(next);
              } else if (method.isAnnotationPresent(org.junit.Before.class)) {
                break;
              }
            }
          }
        } catch (Throwable t) {
          break;
        }
      }
    }
    return firstInstanceAfterTestPipeline;
  }

  private static class IntegrationMatcherStore extends Module {
    private static Map<String, Matcher<PipelineResult>> matcherMap = new ConcurrentHashMap<>();

    /**
     * Adds a matcher to the matcher store, to be retrieved by a TestPipelineRunner during
     * pipeline execution.
     *
     * @param m The matcher to add to the store.
     * @return Returns a UUID corresponding to the matcher which can be used in e.g.
     * opts.SetPassVerifier(matcher)
     */
    String addMatcher(Matcher<PipelineResult> m) {
      if (matcherMap == null) {
        matcherMap = new HashMap<>();
      }
      String uid = UUID.randomUUID().toString();
      matcherMap.put(uid, m);
      return uid;
    }

    /**
     * Retrieves a matcher from the matcher store.
     *
     * @param str The UUID of the matcher to return.
     * @return Returns the matcher corresponding to the string passed, or null if it doesn't exist.
     */
    Matcher<PipelineResult> getMatcher(String str) {
      if (matcherMap == null) {
        return null;
      }
      return matcherMap.get(str);
    }

    @Override
    public String getModuleName() {
      return "IntegrationMatcherStore";
    }

    @Override
    public Version version() {
      return new Version(1, 0, 0, "", null, null);
    }

    @Override
    public void setupModule(SetupContext setupContext) {
      SimpleSerializers ser = new SimpleSerializers();
      ser.addSerializer(Matcher.class, (JsonSerializer) new MatcherSerializer());
      SimpleDeserializers des = new SimpleDeserializers();
      des.addDeserializer(Matcher.class, new MatcherDeserializer());
      setupContext.addSerializers(ser);
      setupContext.addDeserializers(des);
    }

    class MatcherSerializer extends JsonSerializer<Matcher<PipelineResult>> {
      @Override
      public void serialize(Matcher<PipelineResult> matcher, JsonGenerator jsonGenerator,
          SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        String uuid = addMatcher(matcher);
        jsonGenerator.writeString(uuid);
      }
    }

    class MatcherDeserializer extends JsonDeserializer<Matcher<PipelineResult>> {

      @Override
      public Matcher<PipelineResult> deserialize(JsonParser jsonParser,
          DeserializationContext deserializationContext)
          throws IOException, JsonProcessingException {
        String uuid = jsonParser.getText();
        return getMatcher(uuid);
      }
    }
  }
}
