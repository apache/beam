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
package org.apache.beam.sdk.options;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link Default} represents a set of annotations that can be used to annotate getter properties
 * on {@link PipelineOptions} with information representing the default value to be returned
 * if no value is specified.
 */
public @interface Default {
  /**
   * This represents that the default of the option is the specified {@link java.lang.Class} value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  public @interface Class {
    java.lang.Class<?> value();
  }

  /**
   * This represents that the default of the option is the specified {@link java.lang.String}
   * value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface String {
    java.lang.String value();
  }

  /**
   * This represents that the default of the option is the specified boolean primitive value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface Boolean {
    boolean value();
  }

  /**
   * This represents that the default of the option is the specified char primitive value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface Character {
    char value();
  }

  /**
   * This represents that the default of the option is the specified byte primitive value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface Byte {
    byte value();
  }
  /**
   * This represents that the default of the option is the specified short primitive value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface Short {
    short value();
  }
  /**
   * This represents that the default of the option is the specified int primitive value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface Integer {
    int value();
  }

  /**
   * This represents that the default of the option is the specified long primitive value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface Long {
    long value();
  }

  /**
   * This represents that the default of the option is the specified float primitive value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface Float {
    float value();
  }

  /**
   * This represents that the default of the option is the specified double primitive value.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface Double {
    double value();
  }

  /**
   * This represents that the default of the option is the specified enum.
   * The value should equal the enum's {@link java.lang.Enum#name() name}.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @SuppressWarnings("JavaLangClash")
  @interface Enum {
    java.lang.String value();
  }

  /**
   * Value must be of type {@link DefaultValueFactory} and have a default constructor.
   * Value is instantiated and then used as a factory to generate the default.
   *
   * <p>See {@link DefaultValueFactory} for more details.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @interface InstanceFactory {
    java.lang.Class<? extends DefaultValueFactory<?>> value();
  }
}
