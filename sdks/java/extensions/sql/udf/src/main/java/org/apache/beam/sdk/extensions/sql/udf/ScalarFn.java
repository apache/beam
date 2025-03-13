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
package org.apache.beam.sdk.extensions.sql.udf;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A scalar function that can be executed as part of a SQL query. Subclasses must contain exactly
 * one method annotated with {@link ApplyMethod}, which will be applied to the SQL function
 * arguments at runtime.
 *
 * <p>For example:
 *
 * <pre><code>
 * public class IncrementFn extends ScalarFn {
 *  {@literal @ApplyMethod}
 *   public Long increment(Long i) {
 *     return i + 1;
 *   }
 * }
 * </code></pre>
 */
public abstract class ScalarFn implements Serializable {
  /**
   * Annotates the single method in a {@link ScalarFn} implementation that is to be applied to SQL
   * function arguments.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  public static @interface ApplyMethod {}
}
