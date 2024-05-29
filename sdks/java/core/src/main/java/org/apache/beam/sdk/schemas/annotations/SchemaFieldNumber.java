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
package org.apache.beam.sdk.schemas.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.Nonnull;

/**
 * When used on a {@link org.apache.beam.sdk.schemas.JavaFieldSchema POJO} field, a {@link
 * org.apache.beam.sdk.schemas.JavaBeanSchema Java Bean} getter, or an {@link
 * org.apache.beam.sdk.schemas.AutoValueSchema AutoValue} getter, the generated field will have the
 * specified index. There cannot be "gaps" in field numbers, or schema inference will fail. If used,
 * all fields (or getters in the case of a bean) must be annotated.
 *
 * <p>The annotation takes a String as an argument, but this has to be an Integer-parsable String.
 * Otherwise the pipeline will throw a RuntimeException.
 *
 * <p>For example, say we have a Java POJO with a field that we want in our schema but under a
 * different name:
 *
 * <pre><code>
 *  {@literal @}DefaultSchema(JavaFieldSchema.class)
 *   class MyClass {
 *     {@literal @}SchemaFieldNumber("1")
 *     public String user;
 *
 *    {@literal @}SchemaFieldNumber("0")
 *     public int ageInYears;
 *   }
 * </code></pre>
 *
 * <p>The resulting schema will have ageInYears first followed by user.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public @interface SchemaFieldNumber {

  /** The name to use for the generated schema field. */
  @Nonnull
  String value();
}
