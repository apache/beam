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
 * Can be put on a constructor or a static method, in which case that constructor or method will be
 * used to created instance of the class by Beam's schema code.
 *
 * <p>For example, the following Java POJO.
 *
 * <pre><code>
 * {@literal @}DefaultSchema(JavaBeanSchema.class)
 *  class MyClass {
 *    public final String user;
 *    public final int age;
 *
 *   {@literal @}SchemaCreate
 *    public MyClass(String user, int age) {
 *      this.user = user;
 *      this.age = age;
 *    }
 *  }
 * </code></pre>
 *
 * <p>This tells Beam that this constructor can be used to construct instances. Beam will match up
 * the names of the constructor arguments to schema fields in order to decide how to create the
 * class from a Row.
 *
 * <p>This can also be used to annotate a static factory method on the class. For example:
 *
 * <pre><code>
 * {@literal @}DefaultSchema(JavaBeanSchema.class)
 *  class MyClass {
 *    public final String user;
 *    public final int age;
 *
 *    private MyClass(String user, int age) { this.user = user; this.age = age; }
 *
 *   {@literal @}SchemaCreate
 *    public static MyClass create(String user, int age) {
 *      return new MyClass(user, age);
 *    }
 * }
 * </code></pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.CONSTRUCTOR, ElementType.METHOD})
@SuppressWarnings("rawtypes")
@Experimental(Kind.SCHEMAS)
public @interface SchemaCreate {}
