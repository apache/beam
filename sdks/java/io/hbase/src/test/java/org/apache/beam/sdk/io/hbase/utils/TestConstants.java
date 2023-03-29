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
package org.apache.beam.sdk.io.hbase.utils;

import java.nio.charset.StandardCharsets;

/** <b>Internal only:</b> Constants used for testing purposes. */
public class TestConstants {
  // Base timestamp, assumed to be in milliseconds.
  public static long timeT = 123456000;

  public static byte[] rowKey = "row-key-1".getBytes(StandardCharsets.UTF_8);

  public static byte[] colFamily = "cf".getBytes(StandardCharsets.UTF_8);
  public static byte[] colQualifier = "col1".getBytes(StandardCharsets.UTF_8);
  public static byte[] value = "val-1".getBytes(StandardCharsets.UTF_8);

  public static byte[] rowKey2 = "row-key-2".getBytes(StandardCharsets.UTF_8);
  public static byte[] colFamily2 = "cf2".getBytes(StandardCharsets.UTF_8);
  public static byte[] colQualifier2 = "col2".getBytes(StandardCharsets.UTF_8);
  public static byte[] value2 = "long-value-2".getBytes(StandardCharsets.UTF_8);
}
