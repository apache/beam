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
package org.apache.beam.sdk.extensions.sql;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;

/**
 * Interface to create a UDF in Beam SQL.
 *
 * <p>A static method {@code eval} is required. Here is an example:
 *
 * <blockquote>
 *
 * <pre>
 * public static class MyLeftFunction {
 *   public String eval(
 *       &#64;Parameter(name = "s") String s,
 *       &#64;Parameter(name = "n", optional = true) Integer n) {
 *     return s.substring(0, n == null ? 1 : n);
 *   }
 * }</pre>
 *
 * </blockquote>
 *
 * <p>The first parameter is named "s" and is mandatory, and the second parameter is named "n" and
 * is optional(always NULL if not specified).
 */
@Experimental
public interface BeamSqlUdf extends Serializable {
  String UDF_METHOD = "eval";
}
