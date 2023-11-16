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
package org.apache.beam.sdk.io.aws2.s3;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Customer provided key for use with Amazon S3 server-side encryption. */
@JsonInclude(value = JsonInclude.Include.NON_EMPTY)
@JsonDeserialize(builder = SSECustomerKey.Builder.class)
public class SSECustomerKey {

  private final @Nullable String key;
  private final @Nullable String algorithm;
  private final @Nullable String md5;

  private SSECustomerKey(Builder builder) {
    checkArgument(
        (builder.key == null && builder.algorithm == null)
            || (builder.key != null && builder.algorithm != null),
        "Encryption key and algorithm for SSE-C encryption must be specified in pairs");
    key = builder.key;
    algorithm = builder.algorithm;
    md5 =
        builder.md5 == null && key != null
            ? Base64.getEncoder()
                .encodeToString(
                    DigestUtils.md5(
                        Base64.getDecoder().decode(key.getBytes(StandardCharsets.UTF_8))))
            : builder.md5;
  }

  public @Nullable String getKey() {
    return key;
  }

  public @Nullable String getAlgorithm() {
    return algorithm;
  }

  public @Nullable String getMD5() {
    return md5;
  }

  public static Builder builder() {
    return new Builder();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder {

    private @Nullable String key;
    private @Nullable String algorithm;
    private @Nullable String md5;

    private Builder() {}

    public Builder key(@Nullable String key) {
      this.key = key;
      return this;
    }

    public Builder algorithm(@Nullable String algorithm) {
      this.algorithm = algorithm;
      return this;
    }

    public Builder md5(@Nullable String md5) {
      this.md5 = md5;
      return this;
    }

    public SSECustomerKey build() {
      return new SSECustomerKey(this);
    }
  }
}
