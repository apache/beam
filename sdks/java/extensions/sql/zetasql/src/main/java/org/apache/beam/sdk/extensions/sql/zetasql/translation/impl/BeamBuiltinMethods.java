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
package org.apache.beam.sdk.extensions.sql.zetasql.translation.impl;

import java.lang.reflect.Method;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.tree.Types;

/** BeamBuiltinMethods. */
public class BeamBuiltinMethods {
  public static final Method STARTS_WITH_METHOD =
      Types.lookupMethod(StringFunctions.class, "startsWith", String.class, String.class);

  public static final Method ENDS_WITH_METHOD =
      Types.lookupMethod(StringFunctions.class, "endsWith", String.class, String.class);

  public static final Method LIKE_METHOD =
      Types.lookupMethod(StringFunctions.class, "like", String.class, String.class);

  public static final Method CONCAT_METHOD =
      Types.lookupMethod(
          StringFunctions.class,
          "concat",
          String.class,
          String.class,
          String.class,
          String.class,
          String.class);

  public static final Method REPLACE_METHOD =
      Types.lookupMethod(
          StringFunctions.class, "replace", String.class, String.class, String.class);

  public static final Method TRIM_METHOD =
      Types.lookupMethod(StringFunctions.class, "trim", String.class, String.class);

  public static final Method LTRIM_METHOD =
      Types.lookupMethod(StringFunctions.class, "ltrim", String.class, String.class);

  public static final Method RTRIM_METHOD =
      Types.lookupMethod(StringFunctions.class, "rtrim", String.class, String.class);

  public static final Method SUBSTR_METHOD =
      Types.lookupMethod(StringFunctions.class, "substr", String.class, long.class, long.class);

  public static final Method REVERSE_METHOD =
      Types.lookupMethod(StringFunctions.class, "reverse", String.class);

  public static final Method CHAR_LENGTH_METHOD =
      Types.lookupMethod(StringFunctions.class, "charLength", String.class);

  public static final Method TIMESTAMP_METHOD =
      Types.lookupMethod(TimestampFunctions.class, "timestamp", String.class, String.class);

  public static final Method DATE_METHOD =
      Types.lookupMethod(DateFunctions.class, "date", Integer.class, Integer.class, Integer.class);
}
