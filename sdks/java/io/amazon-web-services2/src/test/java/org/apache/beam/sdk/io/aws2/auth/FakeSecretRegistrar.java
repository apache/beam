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
package org.apache.beam.sdk.io.aws2.auth;

import com.google.auto.service.AutoService;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.util.Secret;
import org.apache.beam.sdk.util.SecretRegistrar;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Test {@link SecretRegistrar} registering {@code FakeAwsTestSecretManager}. A spec {@code
 * {"value": "x"}} resolves to {@code x}. Counts how often secrets are fetched.
 */
@AutoService(SecretRegistrar.class)
public class FakeSecretRegistrar implements SecretRegistrar {
  public static final String NAME = "FakeAwsTestSecretManager";
  public static final AtomicInteger FETCHES = new AtomicInteger();

  @Override
  public Map<String, SecretFactory> getSecretFactories() {
    return ImmutableMap.of(NAME, spec -> new FakeSecret(spec.get("value")));
  }

  /** Spec for a secret with the given value. */
  public static String spec(String value) {
    return "{\"value\": \"" + value + "\"}";
  }

  private static class FakeSecret extends Secret {
    private final String value;

    FakeSecret(String value) {
      this.value = value;
    }

    @Override
    public byte[] getSecretBytes() {
      FETCHES.incrementAndGet();
      return value.getBytes(StandardCharsets.UTF_8);
    }
  }
}
