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
package org.apache.beam.sdk.io.gcp.testing;

import com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;

public class BigtableUtils {

  public static ByteString byteString(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  public static ByteString byteStringUtf8(String s) {
    return ByteString.copyFromUtf8(s);
  }

  public static byte[] floatToByteArray(float number) {
    return Ints.toByteArray(Float.floatToIntBits(number));
  }

  public static byte[] longToByteArray(long number) {
    return Longs.toByteArray(number);
  }

  public static byte[] doubleToByteArray(double number) {
    return Longs.toByteArray(Double.doubleToLongBits(number));
  }

  public static byte[] booleanToByteArray(boolean condition) {
    return condition ? new byte[] {1} : new byte[] {0};
  }
}
