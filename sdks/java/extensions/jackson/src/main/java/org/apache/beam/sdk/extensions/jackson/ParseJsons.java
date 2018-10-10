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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Failure;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * {@link PTransform} for parsing JSON {@link String Strings}. Parse {@link PCollection} of {@link
 * String Strings} in JSON format into a {@link PCollection} of objects represented by those JSON
 * {@link String Strings} using Jackson.
 */
public class ParseJsons<OutputT> extends PTransform<PCollection<String>, PCollection<OutputT>> {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();
  private static final TupleTag<Failure<IOException, String>> FAILURE_TAG =
      new TupleTag<Failure<IOException, String>>() {};

  private final Class<? extends OutputT> outputClass;
  private ObjectMapper customMapper;

  /**
   * Creates a {@link ParseJsons} {@link PTransform} that will parse JSON {@link String Strings}
   * into a {@code PCollection<OutputT>} using a Jackson {@link ObjectMapper}.
   */
  public static <OutputT> ParseJsons<OutputT> of(Class<? extends OutputT> outputClass) {
    return new ParseJsons<>(outputClass);
  }

  private ParseJsons(Class<? extends OutputT> outputClass) {
    this.outputClass = outputClass;
  }

  /** Use custom Jackson {@link ObjectMapper} instead of the default one. */
  public ParseJsons<OutputT> withMapper(ObjectMapper mapper) {
    ParseJsons<OutputT> newTransform = new ParseJsons<>(outputClass);
    newTransform.customMapper = mapper;
    return newTransform;
  }

  private OutputT readValue(String input) throws IOException {
    ObjectMapper mapper = Optional.ofNullable(customMapper).orElse(DEFAULT_MAPPER);
    return mapper.readValue(input, outputClass);
  }

  @Override
  public PCollection<OutputT> expand(PCollection<String> input) {
    return input.apply(
        MapElements.via(
            new SimpleFunction<String, OutputT>() {
              @Override
              public OutputT apply(String input) {
                try {
                  return readValue(input);
                } catch (IOException e) {
                  throw new UncheckedIOException(
                      "Failed to parse a " + outputClass.getName() + " from JSON value: " + input,
                      e);
                }
              }
            }));
  }

  /** Return the {@link TupleTag} associated with parsing failures. */
  public static TupleTag<Failure<IOException, String>> failureTag() {
    return FAILURE_TAG;
  }

  /**
   * Sets a {@link TupleTag} to associate with successes, converting this {@link PTransform} into
   * one that returns a {@link PCollectionTuple} and catches any {@link IOException} raised while
   * parsing JSON.
   *
   * <p>Because the input type is static ({@code String}), the failure type is also static and users
   * do not need to supply a failure tag. Instead, failures are always associated with the tag
   * returned by static method {@link ParseJsons#failureTag()}.
   *
   * <p>Example:
   *
   * <pre>{@code
   * PCollection<String> strings = ...;
   * TupleTag<Foo> parsedFoosTag = new TupleTag<>();
   *
   * PCollection<Foo> foos = strings
   *   .apply(ParseJsons
   *     .of(Foo.class)
   *     .withSuccessTag(parsedFoosTag))
   *   .apply(PTransform.compose((PCollectionTuple pcs) -> {
   *     pcs.get(ParseJsons.failureTag()).apply(new MyErrorOutputTransform());
   *     return pcs.get(parsedFoosTag);
   *   }));
   * }</pre>
   */
  public WithFailures withSuccessTag(TupleTag<OutputT> successTag) {
    return new WithFailures(successTag);
  }

  /**
   * Variant of {@link ParseJsons} that that catches {@link IOException} raised while parsing JSON,
   * wrapping success and failure collections in a {@link PCollectionTuple}.
   */
  public class WithFailures extends PTransform<PCollection<String>, PCollectionTuple> {
    private final TupleTag<OutputT> successTag;

    public WithFailures(TupleTag<OutputT> successTag) {
      this.successTag = successTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<String> input) {
      return input.apply(
          MapElements.into(new TypeDescriptor<OutputT>() {})
              .via(
                  Contextful.fn(
                      new Contextful.Fn<String, OutputT>() {
                        @Override
                        public OutputT apply(String input, Contextful.Fn.Context c)
                            throws IOException {
                          return readValue(input);
                        }
                      },
                      Requirements.empty()))
              .withSuccessTag(successTag)
              .withFailureTag(FAILURE_TAG));
    }
  }
}
