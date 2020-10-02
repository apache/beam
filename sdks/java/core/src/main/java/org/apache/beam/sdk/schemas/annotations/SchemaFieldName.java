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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * When used on a {@link org.apache.beam.sdk.schemas.JavaFieldSchema POJO} field, a {@link
 * org.apache.beam.sdk.schemas.JavaBeanSchema Java Bean} getter, or an {@link
 * org.apache.beam.sdk.schemas.AutoValueSchema AutoValue} getter, the specified name is used for the
 * generated schema field.
 *
 * <p>For example, say we have a Java POJO with a field that we want in our schema but under a
 * different name:
 *
 * <pre><code>
 *  {@literal @}DefaultSchema(JavaFieldSchema.class)
 *   class MyClass {
 *     public String user;
 *
 *    {@literal @}SchemaFieldName("age")
 *     public int ageInYears;
 *   }
 * </code></pre>
 *
 * <p>The resulting schema will have fields named "user" and "age."
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@SuppressWarnings("rawtypes")
@Experimental(Kind.SCHEMAS)
public @interface SchemaFieldName {

  /** The name to use for the generated schema field. */
  @Nonnull
  String value();
}
