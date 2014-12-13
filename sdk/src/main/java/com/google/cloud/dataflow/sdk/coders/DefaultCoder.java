/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a default {@link Coder} class to handle encoding and decoding
 * instances of the annotated class.
 *
 * <p> The specified {@code Coder} must implement a function with the following
 * signature:
 * <pre>{@code
 * public static Coder<T> of(Class<T> clazz) {...}
 * }</pre>
 *
 * <p> For example, to configure the use of Java serialization as the default
 * for a class, annotate the class to use
 * {@link com.google.cloud.dataflow.sdk.coders.SerializableCoder} as follows:the
 *
 * <pre><code>
 * {@literal @}DefaultCoder(SerializableCoder.class)
 * public class MyCustomDataType {
 *   // ...
 * }
 * </code></pre>
 *
 * <p> Similarly, to configure the use of
 * {@link com.google.cloud.dataflow.sdk.coders.AvroCoder} as the default:
 * <pre><code>
 * {@literal @}DefaultCoder(AvroCoder.class)
 * public class MyCustomDataType {
 *   public MyCustomDataType() {}   // Avro requires an empty constructor.
 *   // ...
 * }
 * </code></pre>
 *
 * <p> Coders specified explicitly via
 * {@link com.google.cloud.dataflow.sdk.values.PCollection#setCoder(Coder)
 *  PCollection.setCoder}
 * take precedence, followed by Coders registered at runtime via
 * {@link CoderRegistry#registerCoder}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SuppressWarnings("rawtypes")
public @interface DefaultCoder {
  Class<? extends Coder> value();
}
