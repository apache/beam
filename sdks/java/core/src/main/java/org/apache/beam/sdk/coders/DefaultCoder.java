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
package org.apache.beam.sdk.coders;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.beam.sdk.values.PCollection;

/**
 * The {@link DefaultCoder} annotation
 * specifies a default {@link Coder} class to handle encoding and decoding
 * instances of the annotated class.
 *
 * <p>The specified {@link Coder} must satisfy the requirements of
 * {@link CoderProviders#fromStaticMethods}. Two classes provided by the SDK that
 * are intended for use with this annotation include {@link SerializableCoder}
 * and {@link AvroCoder}.
 *
 * <p>To configure the use of Java serialization as the default
 * for a class, annotate the class to use
 * {@link SerializableCoder} as follows:
 *
 * <pre><code>{@literal @}DefaultCoder(SerializableCoder.class)
 * public class MyCustomDataType implements Serializable {
 *   // ...
 * }</code></pre>
 *
 * <p>Similarly, to configure the use of
 * {@link AvroCoder} as the default:
 * <pre><code>{@literal @}DefaultCoder(AvroCoder.class)
 * public class MyCustomDataType {
 *   public MyCustomDataType() {}  // Avro requires an empty constructor.
 *   // ...
 * }</code></pre>
 *
 * <p>Coders specified explicitly via
 * {@link PCollection#setCoder}
 * take precedence, followed by Coders registered at runtime via
 * {@link CoderRegistry#registerCoder}. See {@link CoderRegistry} for a more detailed discussion
 * of the precedence rules.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@SuppressWarnings("rawtypes")
public @interface DefaultCoder {
  Class<? extends Coder> value();
}
