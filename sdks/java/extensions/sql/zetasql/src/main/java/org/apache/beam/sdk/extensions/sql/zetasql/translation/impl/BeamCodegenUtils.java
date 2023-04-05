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

import java.io.UnsupportedEncodingException;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils;
import org.joda.time.DateTime;

/** BeamCodegenUtils. */
@Internal
public class BeamCodegenUtils {
  // convert bytes to String in UTF8 encoding.
  public static String toStringUTF8(byte[] bytes) {
    try {
      return new String(bytes, "UTF8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String toStringTimestamp(long timestamp) {
    return DateTimeUtils.formatTimestampWithTimeZone(new DateTime(timestamp));
  }
}
