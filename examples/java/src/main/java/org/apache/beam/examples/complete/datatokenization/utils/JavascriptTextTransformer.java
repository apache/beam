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
package org.apache.beam.examples.complete.datatokenization.utils;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;

/**
 * A Text UDF Transform Function. Note that this class's implementation is not threadsafe
 */
@SuppressWarnings({
    "initialization.fields.uninitialized",
    "method.invocation.invalid",
    "dereference.of.nullable",
    "argument.type.incompatible",
    "assignment.type.incompatible",
    "return.type.incompatible"
})
@AutoValue
public abstract class JavascriptTextTransformer {

  /**
   * The {@link FailsafeJavascriptUdf} class processes user-defined functions is a fail-safe manner
   * by maintaining the original payload post-transformation and outputting to a dead-letter on
   * failure.
   */
  @AutoValue
  public abstract static class FailsafeJavascriptUdf<T>
      extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {
    public abstract @Nullable String fileSystemPath();

    public abstract @Nullable String functionName();

    public abstract TupleTag<FailsafeElement<T, String>> successTag();

    public abstract TupleTag<FailsafeElement<T, String>> failureTag();

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_JavascriptTextTransformer_FailsafeJavascriptUdf.Builder<>();
    }

    private Counter successCounter =
        Metrics.counter(FailsafeJavascriptUdf.class, "udf-transform-success-count");

    private Counter failedCounter =
        Metrics.counter(FailsafeJavascriptUdf.class, "udf-transform-failed-count");

    /** Builder for {@link FailsafeJavascriptUdf}. */
    @AutoValue.Builder
    public abstract static class Builder<T> {
      public abstract Builder<T> setFileSystemPath(@Nullable String fileSystemPath);

      public abstract Builder<T> setFunctionName(@Nullable String functionName);

      public abstract Builder<T> setSuccessTag(TupleTag<FailsafeElement<T, String>> successTag);

      public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

      public abstract FailsafeJavascriptUdf<T> build();
    }

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> elements) {
      return elements.apply(
          "ProcessUdf",
          ParDo.of(
                  new DoFn<FailsafeElement<T, String>, FailsafeElement<T, String>>() {
                    private JavascriptRuntime javascriptRuntime;

                    @Setup
                    public void setup() {
                      if (fileSystemPath() != null && functionName() != null) {
                        javascriptRuntime = getJavascriptRuntime(fileSystemPath(), functionName());
                      }
                    }

                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      FailsafeElement<T, String> element = context.element();
                      String payloadStr = element.getPayload();

                      try {
                        if (javascriptRuntime != null) {
                          payloadStr = javascriptRuntime.invoke(payloadStr);
                        }

                        if (!Strings.isNullOrEmpty(payloadStr)) {
                          context.output(
                              FailsafeElement.of(element.getOriginalPayload(), payloadStr));
                          successCounter.inc();
                        }
                      } catch (Exception e) {
                        context.output(
                            failureTag(),
                            FailsafeElement.of(element)
                                .setErrorMessage(e.getMessage())
                                .setStacktrace(Throwables.getStackTraceAsString(e)));
                        failedCounter.inc();
                      }
                    }
                  })
              .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }
  }

  /**
   * Grabs code from a FileSystem, loads it into the Nashorn Javascript Engine, and executes
   * Javascript Functions.
   */
  @AutoValue
  public abstract static class JavascriptRuntime {
    @Nullable
    public abstract String fileSystemPath();

    @Nullable
    public abstract String functionName();

    private Invocable invocable;

    /** Builder for {@link JavascriptTextTransformer}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFileSystemPath(@Nullable String fileSystemPath);

      public abstract Builder setFunctionName(@Nullable String functionName);

      public abstract JavascriptRuntime build();
    }

    /**
     * Factory method for generating a JavascriptTextTransformer.Builder.
     *
     * @return a JavascriptTextTransformer builder
     */
    public static Builder newBuilder() {
      return new AutoValue_JavascriptTextTransformer_JavascriptRuntime.Builder();
    }

    /**
     * Gets a cached Javascript Invocable, if fileSystemPath() not set, returns null.
     *
     * @return a Javascript Invocable or null
     */
    @Nullable
    public Invocable getInvocable() throws ScriptException, IOException {

      // return null if no UDF path specified.
      if (Strings.isNullOrEmpty(fileSystemPath())) {
        return null;
      }

      if (invocable == null) {
        Collection<String> scripts = getScripts(fileSystemPath());
        invocable = newInvocable(scripts);
      }
      return invocable;
    }

    /**
     * Factory method for making a new Invocable.
     *
     * @param scripts a collection of javascript scripts encoded with UTF8 to load in
     */
    @Nullable
    private static Invocable newInvocable(Collection<String> scripts) throws ScriptException {
      ScriptEngineManager manager = new ScriptEngineManager();
      ScriptEngine engine = manager.getEngineByName("JavaScript");

      for (String script : scripts) {
        engine.eval(script);
      }

      return (Invocable) engine;
    }

    /**
     * Invokes the UDF with specified data.
     *
     * @param data data to pass to the invocable function
     * @return The data transformed by the UDF in String format
     */
    @Nullable
    public String invoke(String data) throws ScriptException, IOException, NoSuchMethodException {
      Invocable invocable = getInvocable();
      if (invocable == null) {
        throw new RuntimeException("No udf was loaded");
      }

      Object result = getInvocable().invokeFunction(functionName(), data);
      if (result == null || ScriptObjectMirror.isUndefined(result)) {
        return null;

      } else if (result instanceof String) {
        return (String) result;

      } else {
        String className = result.getClass().getName();
        throw new RuntimeException(
            "UDF Function did not return a String. Instead got: " + className);
      }
    }

    /**
     * Loads into memory scripts from a File System from a given path. Supports any file system that
     * {@link FileSystems} supports.
     *
     * @return a collection of scripts loaded as UF8 Strings
     */
    private static Collection<String> getScripts(String path) throws IOException {
      MatchResult result = FileSystems.match(path);
      checkArgument(
          result.status() == Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + path);

      List<String> scripts =
          result.metadata().stream()
              .filter(metadata -> metadata.resourceId().getFilename().endsWith(".js"))
              .map(Metadata::resourceId)
              .map(
                  resourceId -> {
                    try (Reader reader =
                        Channels.newReader(
                            FileSystems.open(resourceId), StandardCharsets.UTF_8.name())) {
                      return CharStreams.toString(reader);
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  })
              .collect(Collectors.toList());

      return scripts;
    }
  }

  /**
   * Retrieves a {@link JavascriptRuntime} configured to invoke the specified function within the
   * script. If either the fileSystemPath or functionName is null or empty, this method will return
   * null indicating that a runtime was unable to be created within the given parameters.
   *
   * @param fileSystemPath The file path to the JavaScript file to execute.
   * @param functionName The function name which will be invoked within the JavaScript script.
   * @return The {@link JavascriptRuntime} instance.
   */
  @Nullable
  private static JavascriptRuntime getJavascriptRuntime(
      String fileSystemPath, String functionName) {
    JavascriptRuntime javascriptRuntime = null;

    if (!Strings.isNullOrEmpty(fileSystemPath) && !Strings.isNullOrEmpty(functionName)) {
      javascriptRuntime =
          JavascriptRuntime.newBuilder()
              .setFunctionName(functionName)
              .setFileSystemPath(fileSystemPath)
              .build();
    }

    return javascriptRuntime;
  }
}
