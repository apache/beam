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
package org.apache.beam.sdk.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import java.math.BigDecimal;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.beam.sdk.util.RowJson.UnsupportedRowJsonException;

/**
 * Contains utilities for extracting primitive values from JSON nodes.
 *
 * <p>Performs validation and rejects values which are out of bounds.
 */
class RowJsonValueExtractors {

  public interface ValueExtractor<V> {
    V extractValue(JsonNode value);
  }

  /**
   * Extracts byte value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedRowJsonException} if value is out of bounds.
   */
  static ValueExtractor<Byte> byteValueExtractor() {
    return ValidatingValueExtractor.<Byte>builder()
        .setExtractor(jsonNode -> (byte) jsonNode.intValue())
        .setValidator(
            jsonNode ->
                jsonNode.isIntegralNumber()
                    && jsonNode.canConvertToInt()
                    && jsonNode.intValue() >= Byte.MIN_VALUE
                    && jsonNode.intValue() <= Byte.MAX_VALUE)
        .build();
  }

  /**
   * Extracts short value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedRowJsonException} if value is out of bounds.
   */
  static ValueExtractor<Short> shortValueExtractor() {
    return ValidatingValueExtractor.<Short>builder()
        .setExtractor(jsonNode -> (short) jsonNode.intValue())
        .setValidator(
            jsonNode ->
                jsonNode.isIntegralNumber()
                    && jsonNode.canConvertToInt()
                    && jsonNode.intValue() >= Short.MIN_VALUE
                    && jsonNode.intValue() <= Short.MAX_VALUE)
        .build();
  }

  /**
   * Extracts int value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedRowJsonException} if value is out of bounds.
   */
  static ValueExtractor<Integer> intValueExtractor() {
    return ValidatingValueExtractor.<Integer>builder()
        .setExtractor(JsonNode::intValue)
        .setValidator(jsonNode -> jsonNode.isIntegralNumber() && jsonNode.canConvertToInt())
        .build();
  }

  /**
   * Extracts long value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedRowJsonException} if value is out of bounds.
   */
  static ValueExtractor<Long> longValueExtractor() {
    return ValidatingValueExtractor.<Long>builder()
        .setExtractor(JsonNode::longValue)
        .setValidator(jsonNode -> jsonNode.isIntegralNumber() && jsonNode.canConvertToLong())
        .build();
  }

  /**
   * Extracts float value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedRowJsonException} if value is out of bounds.
   */
  static ValueExtractor<Float> floatValueExtractor() {
    return ValidatingValueExtractor.<Float>builder()
        .setExtractor(JsonNode::floatValue)
        .setValidator(
            jsonNode ->
                jsonNode.isFloat()

                    // Either floating number which allows lossless conversion to float
                    || (jsonNode.isFloatingPointNumber()
                        && jsonNode.doubleValue() == (double) (float) jsonNode.doubleValue())

                    // Or an integer number which allows lossless conversion to float
                    || (jsonNode.isIntegralNumber()
                        && jsonNode.canConvertToInt()
                        && jsonNode.asInt() == (int) (float) jsonNode.asInt()))
        .build();
  }

  /**
   * Extracts double value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedRowJsonException} if value is out of bounds.
   */
  static ValueExtractor<Double> doubleValueExtractor() {
    return ValidatingValueExtractor.<Double>builder()
        .setExtractor(JsonNode::doubleValue)
        .setValidator(
            jsonNode ->
                jsonNode.isDouble()

                    // Either a long number which allows lossless conversion to float
                    || (jsonNode.isIntegralNumber()
                        && jsonNode.canConvertToLong()
                        && jsonNode.asLong() == (long) (double) jsonNode.asInt())

                    // Or a decimal number which allows lossless conversion to float
                    || (jsonNode.isFloatingPointNumber()
                        && jsonNode
                            .decimalValue()
                            .equals(BigDecimal.valueOf(jsonNode.doubleValue()))))
        .build();
  }

  /**
   * Extracts boolean value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedRowJsonException} if value is out of bounds.
   */
  static ValueExtractor<Boolean> booleanValueExtractor() {
    return ValidatingValueExtractor.<Boolean>builder()
        .setExtractor(JsonNode::booleanValue)
        .setValidator(JsonNode::isBoolean)
        .build();
  }

  /**
   * Extracts string value from the JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedRowJsonException} if value is out of bounds.
   */
  static ValueExtractor<String> stringValueExtractor() {
    return ValidatingValueExtractor.<String>builder()
        .setExtractor(JsonNode::textValue)
        .setValidator(JsonNode::isTextual)
        .build();
  }

  /**
   * Extracts BigDecimal from the JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedRowJsonException} if value is out of bounds.
   */
  static ValueExtractor<BigDecimal> decimalValueExtractor() {
    return ValidatingValueExtractor.<BigDecimal>builder()
        .setExtractor(JsonNode::decimalValue)
        .setValidator(jsonNode -> jsonNode.isNumber())
        .build();
  }

  @AutoValue
  public abstract static class ValidatingValueExtractor<W> implements ValueExtractor<W> {

    abstract Predicate<JsonNode> validator();

    abstract Function<JsonNode, W> extractor();

    static <T> Builder<T> builder() {
      return new AutoValue_RowJsonValueExtractors_ValidatingValueExtractor.Builder<>();
    }

    @Override
    public W extractValue(JsonNode value) {
      if (!validator().test(value)) {
        throw new UnsupportedRowJsonException(
            "Value \""
                + value.asText()
                + "\" "
                + "is out of range for the type of the field defined in the row schema.");
      }

      return extractor().apply(value);
    }

    @AutoValue.Builder
    abstract static class Builder<W> {
      abstract Builder<W> setValidator(Predicate<JsonNode> validator);

      abstract Builder<W> setExtractor(Function<JsonNode, W> extractor);

      abstract ValidatingValueExtractor<W> build();
    }
  }
}
