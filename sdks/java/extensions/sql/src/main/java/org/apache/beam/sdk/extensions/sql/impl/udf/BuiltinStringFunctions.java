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

import com.google.auto.service.AutoService;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;

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
}
