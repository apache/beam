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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * When used on a POJO field or a JavaBean getter, that field or getter is ignored from the inferred
 * schema.
 *
 * <p>For example, a Java POJO with a field that we don't want included in the schema:
 *
 * <pre><code>
 *  {@literal @}DefaultSchema(JavaBeanSchema.class)
 *   class MyClass {
 *     public String user;
 *     public int age;
 *
 *    {@literal @}SchemaIgnore
 *     public String pleaseDontAddToSchema;
 *   }
 * </code></pre>
 *
 * <p>In this case, the {@code pleaseDontAddToSchema} will be excluded from the schema, and
 * implicitly dropped from calculations.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@SuppressWarnings("rawtypes")
@Experimental(Kind.SCHEMAS)
public @interface SchemaIgnore {}
