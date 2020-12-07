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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.function.Strict;

/** Hash Functions. */
@AutoService(BeamBuiltinFunctionProvider.class)
public class BuiltinHashFunctions extends BeamBuiltinFunctionProvider {

  /**
   * MD5(X)
   *
   * <p>Calculates the MD5 digest and returns the value as a 16 element {@code ByteString}.
   */
  @UDF(
      funcName = "MD5",
      parameterArray = {Schema.TypeName.STRING},
      returnType = Schema.TypeName.BYTES)
  @Strict
  public ByteString md5String(String str) {
    return new ByteString(org.apache.commons.codec.digest.DigestUtils.md5(str));
  }

  /**
   * MD5(X)
   *
   * <p>Calculates the MD5 digest and returns the value as a 16 element {@code ByteString}.
   */
  @UDF(
      funcName = "MD5",
      parameterArray = {Schema.TypeName.BYTES},
      returnType = Schema.TypeName.BYTES)
  @Strict
  public ByteString md5Bytes(byte[] bytes) {
    return new ByteString(org.apache.commons.codec.digest.DigestUtils.md5(bytes));
  }

  /**
   * SHA1(X)
   *
   * <p>Calculates the SHA-1 digest and returns the value as a {@code ByteString}.
   */
  @UDF(
      funcName = "SHA1",
      parameterArray = {Schema.TypeName.STRING},
      returnType = Schema.TypeName.BYTES)
  @Strict
  public ByteString sha1String(String str) {
    return new ByteString(org.apache.commons.codec.digest.DigestUtils.sha1(str));
  }

  /**
   * SHA1(X)
   *
   * <p>Calculates the SHA-1 digest and returns the value as a {@code ByteString}.
   */
  @UDF(
      funcName = "SHA1",
      parameterArray = {Schema.TypeName.BYTES},
      returnType = Schema.TypeName.BYTES)
  @Strict
  public ByteString sha1Bytes(byte[] bytes) {
    return new ByteString(org.apache.commons.codec.digest.DigestUtils.sha1(bytes));
  }

  /**
   * SHA256(X)
   *
   * <p>Calculates the SHA-1 digest and returns the value as a {@code ByteString}.
   */
  @UDF(
      funcName = "SHA256",
      parameterArray = {Schema.TypeName.STRING},
      returnType = Schema.TypeName.BYTES)
  @Strict
  public ByteString sha256String(String str) {
    return new ByteString(org.apache.commons.codec.digest.DigestUtils.sha256(str));
  }

  /**
   * SHA256(X)
   *
   * <p>Calculates the SHA-1 digest and returns the value as a {@code ByteString}.
   */
  @UDF(
      funcName = "SHA256",
      parameterArray = {Schema.TypeName.BYTES},
      returnType = Schema.TypeName.BYTES)
  @Strict
  public ByteString sha256Bytes(byte[] bytes) {
    return new ByteString(org.apache.commons.codec.digest.DigestUtils.sha256(bytes));
  }

  /**
   * SHA512(X)
   *
   * <p>Calculates the SHA-1 digest and returns the value as a {@code ByteString}.
   */
  @UDF(
      funcName = "SHA512",
      parameterArray = {Schema.TypeName.STRING},
      returnType = Schema.TypeName.BYTES)
  @Strict
  public ByteString sha512String(String str) {
    return new ByteString(org.apache.commons.codec.digest.DigestUtils.sha512(str));
  }

  /**
   * SHA512(X)
   *
   * <p>Calculates the SHA-1 digest and returns the value as a {@code ByteString}.
   */
  @UDF(
      funcName = "SHA512",
      parameterArray = {Schema.TypeName.BYTES},
      returnType = Schema.TypeName.BYTES)
  @Strict
  public ByteString sha512Bytes(byte[] bytes) {
    return new ByteString(org.apache.commons.codec.digest.DigestUtils.sha512(bytes));
  }
}
