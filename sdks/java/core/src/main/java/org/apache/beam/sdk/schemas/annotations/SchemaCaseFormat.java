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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;

/**
 * When used on a {@link org.apache.beam.sdk.schemas.JavaFieldSchema POJO}, {@link
 * org.apache.beam.sdk.schemas.JavaBeanSchema Java Bean}, or {@link
 * org.apache.beam.sdk.schemas.AutoValueSchema AutoValue} class the specified case format will be
 * used for all the generated Schema fields.
 *
 * <p>The annotation can also be used on individual POJO fields, and Java Bean and AutoValue getters
 * to change the case format just for those fields.
 *
 * <p>For example, say we have a POJO with fields that we want in our schema but under names with a
 * different case format:
 *
 * <pre><code>
 * {@literal @}DefaultSchema(JavaFieldSchema.class)
 * {@literal @}SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
 *   class MyClass {
 *     public String user;
 *     public int ageInYears;
 *    {@literal @}SchemaCaseFormat(CaseFormat.UPPER_CAMEL)
 *     public boolean knowsJavascript;
 *   }
 * </code></pre>
 *
 * <p>The resulting schema will have fields named "user", "age_in_years", "KnowsJavascript".
 *
 * <p>A common application of this annotation is to make schema fields use the {@code
 * lower_underscore} case format, which is recommended for schemas that will be used across language
 * boundaries.
 *
 * <p><b>NOTE:</b> This annotation assumes that field and method names follow the convention of
 * using the {@code lowerCamel} case format. If that is not the case, we make a best effort to
 * convert the field name but the result is not well defined.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.TYPE})
@SuppressWarnings("rawtypes")
@Experimental(Kind.SCHEMAS)
public @interface SchemaCaseFormat {

  /** The name to use for the generated schema field. */
  @Nonnull
  CaseFormat value();
}
