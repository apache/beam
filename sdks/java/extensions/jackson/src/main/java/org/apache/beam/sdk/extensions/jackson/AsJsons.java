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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link PTransform} for serializing objects to JSON {@link String Strings}.
 * Transforms a {@code PCollection<InputT>} into a {@link PCollection} of JSON
 * {@link String Strings} representing objects in the original {@link PCollection} using Jackson.
 */
public class AsJsons<InputT> extends PTransform<PCollection<InputT>, PCollection<String>> {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  private final Class<? extends InputT> inputClass;
  private ObjectMapper customMapper;

  /**
   * Creates a {@link AsJsons} {@link PTransform} that will transform a {@code PCollection<InputT>}
   * into a {@link PCollection} of JSON {@link String Strings} representing those objects using a
   * Jackson {@link ObjectMapper}.
   */
  public static <OutputT> AsJsons<OutputT> of(Class<? extends OutputT> outputClass) {
    return new AsJsons<>(outputClass);
  }

  private AsJsons(Class<? extends InputT> outputClass) {
    this.inputClass = outputClass;
  }

  /**
   * Use custom Jackson {@link ObjectMapper} instead of the default one.
   */
  public AsJsons<InputT> withMapper(ObjectMapper mapper) {
    AsJsons<InputT> newTransform = new AsJsons<>(inputClass);
    newTransform.customMapper = mapper;
    return newTransform;
  }

  @Override
  public PCollection<String> expand(PCollection<InputT> input) {
    return input.apply(MapElements.via(new SimpleFunction<InputT, String>() {
      @Override
      public String apply(InputT input) {
        try {
          ObjectMapper mapper = Optional.fromNullable(customMapper).or(DEFAULT_MAPPER);
          return mapper.writeValueAsString(input);
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to serialize " + inputClass.getName() + " value: " + input, e);
        }
      }
    }));
  }
}
