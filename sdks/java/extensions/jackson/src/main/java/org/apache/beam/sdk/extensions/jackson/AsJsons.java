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
import com.google.common.base.Optional;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.MapElements.Failure;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * {@link PTransform} for serializing objects to JSON {@link String Strings}. Transforms a {@code
 * PCollection<InputT>} into a {@link PCollection} of JSON {@link String Strings} representing
 * objects in the original {@link PCollection} using Jackson.
 */
public class AsJsons<InputT> extends PTransform<PCollection<InputT>, PCollection<String>> {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  private final Class<? extends InputT> inputClass;
  private ObjectMapper customMapper;
  public static final TupleTag<String> successTag = new TupleTag<>();

  /**
   * Creates a {@link AsJsons} {@link PTransform} that will transform a {@code PCollection<InputT>}
   * into a {@link PCollection} of JSON {@link String Strings} representing those objects using a
   * Jackson {@link ObjectMapper}.
   */
  public static <OutputT> AsJsons<OutputT> of(Class<? extends OutputT> inputClass) {
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

  private SimpleFunction<InputT, String> parseFn =
      new SimpleFunction<InputT, String>() {
        @Override
        public String apply(InputT input) {
          try {
            ObjectMapper mapper = Optional.fromNullable(customMapper).or(DEFAULT_MAPPER);
            return mapper.writeValueAsString(input);
          } catch (IOException e) {
            throw new UncheckedIOException(
                "Failed to serialize " + inputClass.getName() + " value: " + input, e);
          }
        }
      };

  @Override
  public PCollection<String> expand(PCollection<InputT> input) {
    return input.apply(MapElements.via(parseFn));
  }

  /**
   * Sets a {@link TupleTag} to associate with serialization failures, converting this {@link
   * PTransform} into one that returns a {@link PCollectionTuple}.
   *
   * <p>Successes will be associated with static tag {@link AsJsons#successTag} since all {@code
   * AsJsons} outputs are of the same type ({@code String}).
   *
   * <p>Example:
   *
   * <pre>{@code
   * PCollection<Foo> foos = ...;
   * TupleTag<Failure<Foo>> unserializedFoosTag = new TupleTag<>();
   *
   * PCollection<String> strings = foos
   *   .apply(AsJsons
   *     .of(Foo.class)
   *     .withSuccessTag(unserializedFoosTag))
   *   .apply(PTransform.compose((PCollectionTuple pcs) -> {
   *     pcs.get(unserializedFoosTag).apply(new MyErrorOutputTransform());
   *     return pcs.get(AsJsons.successTag);
   *   }));
   * }</pre>
   */
  public WithFailures withFailureTag(TupleTag<Failure<InputT>> failureTag) {
    return new WithFailures(failureTag);
  }

  public class WithFailures extends PTransform<PCollection<InputT>, PCollectionTuple> {
    private final TupleTag<Failure<InputT>> failureTag;

    public WithFailures(TupleTag<Failure<InputT>> failureTag) {
      this.failureTag = failureTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<InputT> input) {
      return input.apply(
          MapElements.via(parseFn)
              .withSuccessTag(successTag)
              .withFailureTag(failureTag, UncheckedIOException.class));
    }
  }
}
