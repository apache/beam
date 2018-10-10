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
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * {@link PTransform} for serializing objects to JSON {@link String Strings}. Transforms a {@code
 * PCollection<InputT>} into a {@link PCollection} of JSON {@link String Strings} representing
 * objects in the original {@link PCollection} using Jackson.
 */
public class AsJsons<InputT> extends PTransform<PCollection<InputT>, PCollection<String>> {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();
  private static final TupleTag<String> SUCCESS_TAG = new TupleTag<String>() {};

  private final Class<? extends InputT> inputClass;
  private ObjectMapper customMapper;

  /**
   * Creates a {@link AsJsons} {@link PTransform} that will transform a {@code PCollection<InputT>}
   * into a {@link PCollection} of JSON {@link String Strings} representing those objects using a
   * Jackson {@link ObjectMapper}.
   */
  public static <InputT> AsJsons<InputT> of(Class<? extends InputT> inputClass) {
    return new AsJsons<>(inputClass);
  }

  private AsJsons(Class<? extends InputT> inputClass) {
    this.inputClass = inputClass;
  }

  /** Use custom Jackson {@link ObjectMapper} instead of the default one. */
  public AsJsons<InputT> withMapper(ObjectMapper mapper) {
    AsJsons<InputT> newTransform = new AsJsons<>(inputClass);
    newTransform.customMapper = mapper;
    return newTransform;
  }

  private String writeValue(InputT input) throws IOException {
    ObjectMapper mapper = Optional.ofNullable(customMapper).orElse(DEFAULT_MAPPER);
    return mapper.writeValueAsString(input);
  }

  @Override
  public PCollection<String> expand(PCollection<InputT> input) {
    return input.apply(
        MapElements.via(
            new SimpleFunction<InputT, String>() {
              @Override
              public String apply(InputT input) {
                try {
                  return writeValue(input);
                } catch (IOException e) {
                  throw new UncheckedIOException(
                      "Failed to serialize " + inputClass.getName() + " value: " + input, e);
                }
              }
            }));
  }

  /** Return the {@link TupleTag} associated with successfully written JSON strings. */
  public static TupleTag<String> successTag() {
    return SUCCESS_TAG;
  }

  /**
   * Sets a {@link TupleTag} to associate with serialization failures, converting this {@link
   * PTransform} into one that returns a {@link PCollectionTuple}.
   *
   * <p>Because the success output type is static ({@code String}), users do not need to supply a
   * success tag. Instead, successes are always associated with the tag returned by static method
   * {@link AsJsons#successTag()}.
   *
   * <p>Example:
   *
   * <pre>{@code
   * PCollection<Foo> foos = ...;
   * TupleTag<Failure<IOException, Foo>> unserializedFoosTag =
   *   new TupleTag<Failure<IOException, Foo>>(){};
   *
   * PCollection<String> strings = foos
   *   .apply(AsJsons
   *     .of(Foo.class)
   *     .withFailureTag(unserializedFoosTag))
   *   .apply(PTransform.compose((PCollectionTuple pcs) -> {
   *     pcs.get(unserializedFoosTag).apply(new MyErrorOutputTransform());
   *     return pcs.get(AsJsons.successTag());
   *   }));
   * }</pre>
   */
  public WithFailures withFailureTag(TupleTag<Failure<IOException, InputT>> failureTag) {
    return new WithFailures(failureTag);
  }

  /**
   * Variant of {@link AsJsons} that that catches {@link IOException} raised while writing JSON,
   * wrapping success and failure collections in a {@link PCollectionTuple}.
   */
  public class WithFailures extends PTransform<PCollection<InputT>, PCollectionTuple> {
    private final TupleTag<Failure<IOException, InputT>> failureTag;

    WithFailures(TupleTag<Failure<IOException, InputT>> failureTag) {
      this.failureTag = failureTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<InputT> input) {
      return input.apply(
          MapElements.into(TypeDescriptors.strings())
              .via(
                  Contextful.fn(
                      new Contextful.Fn<InputT, String>() {
                        @Override
                        public String apply(InputT input, Contextful.Fn.Context c)
                            throws IOException {
                          return writeValue(input);
                        }
                      },
                      Requirements.empty()))
              .withSuccessTag(SUCCESS_TAG)
              .withFailureTag(failureTag));
    }
  }
}
