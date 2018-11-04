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
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/** BuiltinStringFunctions. */
@AutoService(BeamBuiltinFunctionProvider.class)
public class BuiltinStringFunctions extends BeamBuiltinFunctionProvider {

  // return a explicitly null for Boolean has NP_BOOLEAN_RETURN_NULL warning.
  // return null for boolean is not allowed.
  // TODO: handle null input.
  @UDF(
    funcName = "ENDS_WITH",
    parameterArray = {TypeName.STRING},
    returnType = TypeName.STRING
  )
  public Boolean endsWith(String str1, String str2) {
    return str1.endsWith(str2);
  }

  // return a explicitly null for Boolean has NP_BOOLEAN_RETURN_NULL warning.
  // return null for boolean is not allowed.
  // TODO: handle null input.
  @UDF(
    funcName = "STARTS_WITH",
    parameterArray = {TypeName.STRING},
    returnType = TypeName.STRING
  )
  public Boolean startsWith(String str1, String str2) {
    return str1.startsWith(str2);
  }

  @UDF(
    funcName = "LENGTH",
    parameterArray = {TypeName.STRING},
    returnType = TypeName.INT64
  )
  public Long length(String str) {
    if (str == null) {
      return null;
    }
    return (long) str.length();
  }

  @UDF(
    funcName = "LENGTH",
    parameterArray = {TypeName.BYTES},
    returnType = TypeName.INT64
  )
  public Long length(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return (long) bytes.length;
  }

  @UDF(
    funcName = "REVERSE",
    parameterArray = {TypeName.STRING},
    returnType = TypeName.STRING
  )
  public String reverse(String str) {
    if (str == null) {
      return null;
    }
    return new StringBuilder(str).reverse().toString();
  }

  @UDF(
    funcName = "REVERSE",
    parameterArray = {TypeName.BYTES},
    returnType = TypeName.BYTES
  )
  public byte[] reverse(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    byte[] ret = Arrays.copyOf(bytes, bytes.length);
    ArrayUtils.reverse(ret);
    return ret;
  }

  @UDF(
    funcName = "FROM_HEX",
    parameterArray = {TypeName.STRING},
    returnType = TypeName.BYTES
  )
  public byte[] fromHex(String str) throws DecoderException {
    if (str == null) {
      return null;
    }

    return Hex.decodeHex(str.toCharArray());
  }

  @UDF(
    funcName = "TO_HEX",
    parameterArray = {TypeName.BYTES},
    returnType = TypeName.STRING
  )
  public String toHex(byte[] bytes) throws DecoderException {
    if (bytes == null) {
      return null;
    }

    return Hex.encodeHexString(bytes);
  }

  @UDF(
    funcName = "LPAD",
    parameterArray = {TypeName.STRING, TypeName.INT64},
    returnType = TypeName.STRING
  )
  public String lpad(String originalValue, Long returnLength) {
    return lpad(originalValue, returnLength, " ");
  }

  @UDF(
    funcName = "LPAD",
    parameterArray = {TypeName.STRING, TypeName.INT64, TypeName.STRING},
    returnType = TypeName.STRING
  )
  public String lpad(String originalValue, Long returnLength, String pattern) {
    if (originalValue == null || returnLength == null || pattern == null) {
      return null;
    }

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
    returnType = TypeName.BYTES
  )
  public byte[] lpad(byte[] originalValue, Long returnLength) {
    return lpad(originalValue, returnLength, " ".getBytes(UTF_8));
  }

  @UDF(
    funcName = "LPAD",
    parameterArray = {TypeName.BYTES, TypeName.INT64, TypeName.BYTES},
    returnType = TypeName.BYTES
  )
  public byte[] lpad(byte[] originalValue, Long returnLength, byte[] pattern) {
    if (originalValue == null || returnLength == null || pattern == null) {
      return null;
    }
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
    returnType = TypeName.STRING
  )
  public String rpad(String originalValue, Long returnLength) {
    return lpad(originalValue, returnLength, " ");
  }

  @UDF(
    funcName = "RPAD",
    parameterArray = {TypeName.STRING, TypeName.INT64, TypeName.STRING},
    returnType = TypeName.STRING
  )
  public String rpad(String originalValue, Long returnLength, String pattern) {
    if (originalValue == null || returnLength == null || pattern == null) {
      return null;
    }

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
    returnType = TypeName.BYTES
  )
  public byte[] rpad(byte[] originalValue, Long returnLength) {
    return lpad(originalValue, returnLength, " ".getBytes(UTF_8));
  }

  @UDF(
    funcName = "RPAD",
    parameterArray = {TypeName.BYTES, TypeName.INT64, TypeName.BYTES},
    returnType = TypeName.BYTES
  )
  public byte[] rpad(byte[] originalValue, Long returnLength, byte[] pattern) {
    if (originalValue == null || returnLength == null || pattern == null) {
      return null;
    }
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
