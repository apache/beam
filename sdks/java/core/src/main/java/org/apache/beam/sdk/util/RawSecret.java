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
package org.apache.beam.sdk.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link Secret} implementation wrapping a raw secret string or byte array directly. */
public class RawSecret extends Secret {
  private final byte[] secret;

  public RawSecret(byte[] secret) {
    this.secret = secret == null ? new byte[0] : secret.clone();
  }

  public RawSecret(String secret) {
    this.secret = secret == null ? new byte[0] : secret.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public byte[] getSecretBytes() {
    return secret.clone();
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RawSecret)) {
      return false;
    }
    RawSecret other = (RawSecret) obj;
    return Arrays.equals(this.secret, other.secret);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(secret);
  }
}
