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
package org.apache.beam.sdk.io.aws2.schemas;

import static software.amazon.awssdk.core.protocol.MarshallLocation.PAYLOAD;
import static software.amazon.awssdk.core.protocol.MarshallingType.BIG_DECIMAL;
import static software.amazon.awssdk.core.protocol.MarshallingType.BOOLEAN;
import static software.amazon.awssdk.core.protocol.MarshallingType.DOUBLE;
import static software.amazon.awssdk.core.protocol.MarshallingType.FLOAT;
import static software.amazon.awssdk.core.protocol.MarshallingType.INSTANT;
import static software.amazon.awssdk.core.protocol.MarshallingType.INTEGER;
import static software.amazon.awssdk.core.protocol.MarshallingType.LIST;
import static software.amazon.awssdk.core.protocol.MarshallingType.LONG;
import static software.amazon.awssdk.core.protocol.MarshallingType.MAP;
import static software.amazon.awssdk.core.protocol.MarshallingType.SDK_BYTES;
import static software.amazon.awssdk.core.protocol.MarshallingType.SHORT;
import static software.amazon.awssdk.core.protocol.MarshallingType.STRING;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.core.SdkPojo;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.core.traits.ListTrait;
import software.amazon.awssdk.core.traits.LocationTrait;
import software.amazon.awssdk.core.traits.MapTrait;
import software.amazon.awssdk.utils.builder.SdkBuilder;

/**
 * Sample AWS SDK pojo with all supported types. Nested objects as well as collections of objects
 * are tested using "real" AWS model classes.
 */
public class Sample implements SdkPojo, Serializable {
  private static <T> SdkField.Builder<T> sdkField(MarshallingType<T> type) {
    return SdkField.builder(type).traits(LocationTrait.builder().location(PAYLOAD).build());
  }

  private static <T> SdkField.Builder<T> sdkField(
      MarshallingType<? super T> type,
      String name,
      Function<Sample, T> g,
      BiConsumer<Builder, T> s) {
    return sdkField((MarshallingType<T>) type)
        .memberName(name)
        .getter(obj -> g.apply((Sample) obj))
        .setter((obj, val) -> s.accept((Builder) obj, val));
  }

  private static final SdkField<String> STRING_F =
      sdkField(STRING, "String", Sample::stringField, Builder::stringField).build();

  private static final SdkField<Short> SHORT_F =
      sdkField(SHORT, "Short", Sample::shortField, Builder::shortField).build();

  private static final SdkField<Integer> INTEGER_F =
      sdkField(INTEGER, "Integer", Sample::integerField, Builder::integerField).build();

  private static final SdkField<Long> LONG_F =
      sdkField(LONG, "Long", Sample::longField, Builder::longField).build();

  private static final SdkField<Float> FLOAT_F =
      sdkField(FLOAT, "Float", Sample::floatField, Builder::floatField).build();

  private static final SdkField<Double> DOUBLE_F =
      sdkField(DOUBLE, "Double", Sample::doubleField, Builder::doubleField).build();

  private static final SdkField<BigDecimal> DECIMAL_F =
      sdkField(BIG_DECIMAL, "Decimal", Sample::decimalField, Builder::decimalField).build();

  private static final SdkField<Boolean> BOOLEAN_F =
      sdkField(BOOLEAN, "Boolean", Sample::booleanField, Builder::booleanField).build();

  private static final SdkField<Instant> INSTANT_F =
      sdkField(INSTANT, "Instant", Sample::instantField, Builder::instantField).build();

  private static final SdkField<SdkBytes> BYTES_F =
      sdkField(SDK_BYTES, "Bytes", Sample::bytesField, Builder::bytesField).build();

  private static final SdkField<List<String>> LIST_F =
      sdkField(LIST, "List", Sample::listField, Builder::listField)
          .traits(ListTrait.builder().memberFieldInfo(sdkField(STRING).build()).build())
          .build();

  private static final SdkField<Map<String, String>> MAP_F =
      sdkField(MAP, "Map", Sample::mapField, Builder::mapField)
          .traits(MapTrait.builder().valueFieldInfo(sdkField(STRING).build()).build())
          .build();

  private static final List<SdkField<?>> SDK_FIELDS =
      ImmutableList.of(
          STRING_F, SHORT_F, INTEGER_F, LONG_F, FLOAT_F, DOUBLE_F, DECIMAL_F, BOOLEAN_F, INSTANT_F,
          BYTES_F, LIST_F, MAP_F);

  private final String stringField;
  private final Short shortField;
  private final Integer integerField;
  private final Long longField;
  private final Float floatField;
  private final Double doubleField;
  private final BigDecimal decimalField;
  private final Boolean booleanField;
  private final Instant instantField;
  private final SdkBytes bytesField;
  private final List<String> listField;
  private final Map<String, String> mapField;

  private Sample(BuilderImpl builder) {
    this.stringField = builder.stringField;
    this.shortField = builder.shortField;
    this.integerField = builder.integerField;
    this.longField = builder.longField;
    this.floatField = builder.floatField;
    this.doubleField = builder.doubleField;
    this.decimalField = builder.decimalField;
    this.booleanField = builder.booleanField;
    this.instantField = builder.instantField;
    this.bytesField = builder.bytesField;
    this.listField = builder.listField;
    this.mapField = builder.mapField;
  }

  public final String stringField() {
    return stringField;
  }

  public final Short shortField() {
    return shortField;
  }

  public final Integer integerField() {
    return integerField;
  }

  public final Long longField() {
    return longField;
  }

  public final Float floatField() {
    return floatField;
  }

  public final Double doubleField() {
    return doubleField;
  }

  public final BigDecimal decimalField() {
    return decimalField;
  }

  public final Boolean booleanField() {
    return booleanField;
  }

  public final Instant instantField() {
    return instantField;
  }

  public final SdkBytes bytesField() {
    return bytesField;
  }

  public final List<String> listField() {
    return listField;
  }

  public final Map<String, String> mapField() {
    return mapField;
  }

  public static Builder builder() {
    return new BuilderImpl();
  }

  @Override
  public final int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public final boolean equals(Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

  @Override
  public final String toString() {
    return ReflectionToStringBuilder.toString(this);
  }

  @Override
  public final List<SdkField<?>> sdkFields() {
    return SDK_FIELDS;
  }

  public interface Builder extends SdkPojo, SdkBuilder<Builder, Sample> {
    Builder stringField(String stringField);

    Builder shortField(Short shortField);

    Builder integerField(Integer integerField);

    Builder longField(Long longField);

    Builder floatField(Float floatField);

    Builder doubleField(Double doubleField);

    Builder decimalField(BigDecimal decimalField);

    Builder booleanField(Boolean booleanField);

    Builder instantField(Instant instantField);

    Builder bytesField(SdkBytes bytesField);

    Builder listField(List<String> listField);

    Builder mapField(Map<String, String> mapField);
  }

  static final class BuilderImpl implements Builder {
    private String stringField;
    private Short shortField;
    private Integer integerField;
    private Long longField;
    private Float floatField;
    private Double doubleField;
    private BigDecimal decimalField;
    private Boolean booleanField;
    private Instant instantField;
    private SdkBytes bytesField;
    private List<String> listField;
    private Map<String, String> mapField;

    @Override
    public Builder stringField(String stringField) {
      this.stringField = stringField;
      return this;
    }

    @Override
    public Builder shortField(Short shortField) {
      this.shortField = shortField;
      return this;
    }

    @Override
    public Builder integerField(Integer integerField) {
      this.integerField = integerField;
      return this;
    }

    @Override
    public Builder longField(Long longField) {
      this.longField = longField;
      return this;
    }

    @Override
    public Builder floatField(Float floatField) {
      this.floatField = floatField;
      return this;
    }

    @Override
    public Builder doubleField(Double doubleField) {
      this.doubleField = doubleField;
      return this;
    }

    @Override
    public Builder decimalField(BigDecimal decimalField) {
      this.decimalField = decimalField;
      return this;
    }

    @Override
    public Builder booleanField(Boolean booleanField) {
      this.booleanField = booleanField;
      return this;
    }

    @Override
    public Builder instantField(Instant instantField) {
      this.instantField = instantField;
      return this;
    }

    @Override
    public Builder bytesField(SdkBytes bytesField) {
      this.bytesField = bytesField;
      return this;
    }

    @Override
    public Builder listField(List<String> listField) {
      this.listField = listField;
      return this;
    }

    @Override
    public Builder mapField(Map<String, String> mapField) {
      this.mapField = mapField;
      return this;
    }

    @Override
    public Sample build() {
      return new Sample(this);
    }

    @Override
    public List<SdkField<?>> sdkFields() {
      return SDK_FIELDS;
    }
  }
}
