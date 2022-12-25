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
package org.apache.beam.examples.complete.cdap.utils;

import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;

/**
 * Class for converting {@link io.cdap.cdap.api.data.format.StructuredRecord} to human-readable
 * format.
 */
public class StructuredRecordUtils {

  private static final Gson GSON = new Gson();

  /** Converts {@link StructuredRecord} to String json format. */
  public static String structuredRecordToString(StructuredRecord structuredRecord) {
    if (structuredRecord == null) {
      return "{}";
    }
    return GSON.toJson(structuredRecord);
  }
}
