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
 * {@link PTransform} for parsing JSON {@link String Strings}.
 * Parse {@link PCollection} of {@link String Strings} in JSON format into a {@link PCollection} of
 * objects represented by those JSON {@link String Strings} using Jackson.
 */
public class ParseJsons<OutputT> extends PTransform<PCollection<String>, PCollection<OutputT>> {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

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

  /**
   * Use custom Jackson {@link ObjectMapper} instead of the default one.
   */
  public ParseJsons<OutputT> withMapper(ObjectMapper mapper) {
    ParseJsons<OutputT> newTransform = new ParseJsons<>(outputClass);
    newTransform.customMapper = mapper;
    return newTransform;
  }

  @Override
  public PCollection<OutputT> expand(PCollection<String> input) {
    return input.apply(MapElements.via(new SimpleFunction<String, OutputT>() {
      @Override
      public OutputT apply(String input) {
        try {
          ObjectMapper mapper = Optional.fromNullable(customMapper).or(DEFAULT_MAPPER);
          return mapper.readValue(input, outputClass);
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to parse a " + outputClass.getName() + " from JSON value: " + input, e);
        }
      }
    }));
  }
}
