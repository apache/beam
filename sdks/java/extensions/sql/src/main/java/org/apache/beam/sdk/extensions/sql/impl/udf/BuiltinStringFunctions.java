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
package org.apache.beam.sdk.extensions.sql.impl.udf;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.function.Strict;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/** BuiltinStringFunctions. */
@AutoService(BeamBuiltinFunctionProvider.class)
public class BuiltinStringFunctions extends BeamBuiltinFunctionProvider {

  @UDF(
      funcName = "ENDS_WITH",
      parameterArray = {TypeName.STRING},
      returnType = TypeName.STRING)
  @Strict
  public Boolean endsWith(String str1, String str2) {
    return str1.endsWith(str2);
  }

  @UDF(
      funcName = "STARTS_WITH",
      parameterArray = {TypeName.STRING},
      returnType = TypeName.STRING)
  @Strict
  public Boolean startsWith(String str1, String str2) {
    return str1.startsWith(str2);
  }

  @UDF(
      funcName = "LENGTH",
      parameterArray = {TypeName.STRING},
      returnType = TypeName.INT64)
  @Strict
  public Long lengthString(String str) {
    return (long) str.length();
  }

  @UDF(
      funcName = "LENGTH",
      parameterArray = {TypeName.BYTES},
      returnType = TypeName.INT64)
  @Strict
  public Long lengthBytes(byte[] bytes) {
    return (long) bytes.length;
  }

  @UDF(
      funcName = "REVERSE",
      parameterArray = {TypeName.STRING},
      returnType = TypeName.STRING)
  @Strict
  public String reverseString(String str) {
    return new StringBuilder(str).reverse().toString();
  }

  @UDF(
      funcName = "REVERSE",
      parameterArray = {TypeName.BYTES},
      returnType = TypeName.BYTES)
  @Strict
  public ByteString reverseBytes(ByteString byteString) {
    byte[] bytes = byteString.getBytes();
    byte[] ret = Arrays.copyOf(bytes, bytes.length);
    ArrayUtils.reverse(ret);
    return new ByteString(ret);
  }

  @UDF(
      funcName = "FROM_HEX",
      parameterArray = {TypeName.STRING},
      returnType = TypeName.BYTES)
  @Strict
  public ByteString fromHex(String str) {
    try {
      return new ByteString(Hex.decodeHex(str.toCharArray()));
    } catch (DecoderException e) {
      throw new RuntimeException(e);
    }
  }

  @UDF(
      funcName = "TO_HEX",
      parameterArray = {TypeName.BYTES},
      returnType = TypeName.STRING)
  @Strict
  public String toHex(ByteString bytes) {
    return Hex.encodeHexString(bytes.getBytes());
  }

  @UDF(
      funcName = "LPAD",
      parameterArray = {TypeName.STRING, TypeName.INT64},
      returnType = TypeName.STRING)
  @Strict
  public String lpad(String originalValue, Long returnLength) {
    return lpad(originalValue, returnLength, " ");
  }

  @UDF(
      funcName = "LPAD",
      parameterArray = {TypeName.STRING, TypeName.INT64, TypeName.STRING},
      returnType = TypeName.STRING)
  @Strict
  public String lpad(String originalValue, Long returnLength, String pattern) {
    if (returnLength < -1 || pattern.isEmpty()) {
      throw new IllegalArgumentException("returnLength cannot be 0 or pattern cannot be empty.");
    }

    if (originalValue.length() == returnLength) {
      return originalValue;
    } else if (originalValue.length() < returnLength) { // add padding to left
      return StringUtils.leftPad(originalValue, Math.toIntExact(returnLength), pattern);
    } else { // truncating string by str.substring
      // Java String can only hold a string with Integer.MAX_VALUE as longest length.
      return originalValue.substring(0, Math.toIntExact(returnLength));
    }
  }

  @UDF(
      funcName = "LPAD",
      parameterArray = {TypeName.BYTES, TypeName.INT64},
      returnType = TypeName.BYTES)
  @Strict
  public ByteString lpad(ByteString originalValue, Long returnLength) {
    return new ByteString(lpad(originalValue.getBytes(), returnLength, " ".getBytes(UTF_8)));
  }

  @UDF(
      funcName = "LPAD",
      parameterArray = {TypeName.BYTES, TypeName.INT64, TypeName.BYTES},
      returnType = TypeName.BYTES)
  @Strict
  public ByteString lpad(ByteString originalValue, Long returnLength, ByteString pattern) {
    return new ByteString(lpad(originalValue.getBytes(), returnLength, pattern.getBytes()));
  }

  private byte[] lpad(byte[] originalValue, Long returnLength, byte[] pattern) {
    if (returnLength < -1 || pattern.length == 0) {
      throw new IllegalArgumentException("returnLength cannot be 0 or pattern cannot be empty.");
    }

    int returnLengthInt = Math.toIntExact(returnLength);

    if (originalValue.length == returnLengthInt) {
      return originalValue;
    } else if (originalValue.length < returnLengthInt) { // add padding to left
      byte[] ret = new byte[returnLengthInt];
      // step one: pad #(returnLengthInt - originalValue.length) bytes to left side.
      int paddingOff = 0;
      int paddingLeftBytes = returnLengthInt - originalValue.length;
      byteArrayPadding(ret, pattern, paddingOff, paddingLeftBytes);

      // step two: copy originalValue.
      System.arraycopy(
          originalValue, 0, ret, returnLengthInt - originalValue.length, originalValue.length);
      return ret;
    } else { // truncating string by str.substring
      // Java String can only hold a string with Integer.MAX_VALUE as longest length.
      byte[] ret = new byte[returnLengthInt];
      System.arraycopy(originalValue, 0, ret, 0, returnLengthInt);
      return ret;
    }
  }

  @UDF(
      funcName = "RPAD",
      parameterArray = {TypeName.STRING, TypeName.INT64},
      returnType = TypeName.STRING)
  @Strict
  public String rpad(String originalValue, Long returnLength) {
    return lpad(originalValue, returnLength, " ");
  }

  @UDF(
      funcName = "RPAD",
      parameterArray = {TypeName.STRING, TypeName.INT64, TypeName.STRING},
      returnType = TypeName.STRING)
  @Strict
  public String rpad(String originalValue, Long returnLength, String pattern) {
    if (returnLength < -1 || pattern.isEmpty()) {
      throw new IllegalArgumentException("returnLength cannot be 0 or pattern cannot be empty.");
    }

    if (originalValue.length() == returnLength) {
      return originalValue;
    } else if (originalValue.length() < returnLength) { // add padding to right
      return StringUtils.rightPad(originalValue, Math.toIntExact(returnLength), pattern);
    } else { // truncating string by str.substring
      // Java String can only hold a string with Integer.MAX_VALUE as longest length.
      return originalValue.substring(0, Math.toIntExact(returnLength));
    }
  }

  @UDF(
      funcName = "RPAD",
      parameterArray = {TypeName.BYTES, TypeName.INT64},
      returnType = TypeName.BYTES)
  @Strict
  public ByteString rpad(ByteString originalValue, Long returnLength) {
    return new ByteString(lpad(originalValue.getBytes(), returnLength, " ".getBytes(UTF_8)));
  }

  @UDF(
      funcName = "RPAD",
      parameterArray = {TypeName.BYTES, TypeName.INT64, TypeName.BYTES},
      returnType = TypeName.BYTES)
  @Strict
  public ByteString rpad(ByteString originalValue, Long returnLength, ByteString pattern) {
    return new ByteString(rpad(originalValue.getBytes(), returnLength, pattern.getBytes()));
  }

  private byte[] rpad(byte[] originalValue, Long returnLength, byte[] pattern) {
    if (returnLength < -1 || pattern.length == 0) {
      throw new IllegalArgumentException("returnLength cannot be 0 or pattern cannot be empty.");
    }

    int returnLengthInt = Math.toIntExact(returnLength);

    if (originalValue.length == returnLengthInt) {
      return originalValue;
    } else if (originalValue.length < returnLengthInt) { // add padding to right
      byte[] ret = new byte[returnLengthInt];
      // step one: copy originalValue.
      System.arraycopy(originalValue, 0, ret, 0, originalValue.length);

      // step one: pad #(returnLengthInt - originalValue.length) bytes to right side.
      int paddingOff = originalValue.length;
      int paddingLeftBytes = returnLengthInt - originalValue.length;
      byteArrayPadding(ret, pattern, paddingOff, paddingLeftBytes);
      return ret;
    } else { // truncating string by str.substring
      // Java String can only hold a string with Integer.MAX_VALUE as longest length.
      byte[] ret = new byte[returnLengthInt];
      System.arraycopy(originalValue, 0, ret, 0, returnLengthInt);
      return ret;
    }
  }

  private void byteArrayPadding(byte[] dest, byte[] pattern, int paddingOff, int paddingLeftBytes) {
    while (paddingLeftBytes > 0) {
      if (paddingLeftBytes >= pattern.length) {
        // pad the whole pattern
        System.arraycopy(pattern, 0, dest, paddingOff, pattern.length);
        paddingLeftBytes -= pattern.length;
        paddingOff += pattern.length;
      } else {
        System.arraycopy(pattern, 0, dest, paddingOff, paddingLeftBytes);
        paddingLeftBytes = 0;
      }
    }
  }
}
