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

import static org.apache.beam.sdk.io.aws2.auth.FakeSecretRegistrar.FETCHES;
import static org.apache.beam.sdk.io.aws2.auth.FakeSecretRegistrar.NAME;
import static org.apache.beam.sdk.io.aws2.auth.FakeSecretRegistrar.spec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/** Tests for {@link SecretCredentialsProvider}. */
@RunWith(JUnit4.class)
public class SecretCredentialsProviderTest {

  @Before
  public void setUp() {
    SecretCredentialsProvider.clearCache();
    FETCHES.set(0);
  }

  @Test
  public void testResolveBasicCredentials() {
    SecretCredentialsProvider provider =
        SecretCredentialsProvider.create(NAME, spec("key"), spec("secret"));
    AwsCredentials credentials = provider.resolveCredentials();
    assertThat(credentials).isInstanceOf(AwsBasicCredentials.class);
    assertThat(credentials.accessKeyId()).isEqualTo("key");
    assertThat(credentials.secretAccessKey()).isEqualTo("secret");
  }

  @Test
  public void testResolveSessionCredentials() {
    SecretCredentialsProvider provider =
        SecretCredentialsProvider.create(NAME, spec("key"), spec("secret"), spec("token"));
    AwsCredentials credentials = provider.resolveCredentials();
    assertThat(credentials).isInstanceOf(AwsSessionCredentials.class);
    assertThat(credentials.accessKeyId()).isEqualTo("key");
    assertThat(credentials.secretAccessKey()).isEqualTo("secret");
    assertThat(((AwsSessionCredentials) credentials).sessionToken()).isEqualTo("token");
  }

  @Test
  public void testEmptySessionTokenIsIgnored() {
    SecretCredentialsProvider provider =
        SecretCredentialsProvider.create(NAME, spec("key"), spec("secret"), "");
    assertThat(provider.sessionToken()).isNull();
    assertThat(provider.resolveCredentials()).isInstanceOf(AwsBasicCredentials.class);
  }

  @Test
  public void testResolvesLazilyAndCaches() {
    SecretCredentialsProvider provider =
        SecretCredentialsProvider.create(NAME, spec("key"), spec("secret"));
    assertThat(FETCHES.get()).isEqualTo(0);

    provider.resolveCredentials();
    assertThat(FETCHES.get()).isEqualTo(2);

    provider.resolveCredentials();
    assertThat(FETCHES.get()).isEqualTo(2);

    // Another instance with the same specs, e.g. after deserialization, uses the JVM cache.
    SecretCredentialsProvider other =
        SecretCredentialsProvider.create(NAME, spec("key"), spec("secret"));
    assertThat(other.resolveCredentials().accessKeyId()).isEqualTo("key");
    assertThat(FETCHES.get()).isEqualTo(2);
  }

  @Test
  public void testJavaSerializationKeepsOnlySpecs() {
    SecretCredentialsProvider provider =
        SecretCredentialsProvider.create(NAME, spec("key"), spec("secret"), spec("token"));
    provider.resolveCredentials();

    SecretCredentialsProvider copy = SerializableUtils.clone(provider);
    assertThat(copy).isEqualTo(provider);
    assertThat(copy.hashCode()).isEqualTo(provider.hashCode());
    assertThat(copy.resolveCredentials().accessKeyId()).isEqualTo("key");
  }

  @Test
  public void testRejectsAwsSecretsManager() {
    for (String name : new String[] {"AwsSecretsManager", " awssecretsmanager ", "AwsSecret"}) {
      assertThatThrownBy(() -> SecretCredentialsProvider.create(name, spec("k"), spec("s")))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("cannot be AWS Secrets Manager");
    }
  }

  @Test
  public void testRequiresArguments() {
    assertThatThrownBy(() -> SecretCredentialsProvider.create("", spec("k"), spec("s")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SecretCredentialsProvider.create(NAME, "", spec("s")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SecretCredentialsProvider.create(NAME, spec("k"), ""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testUnknownSecretManagerFailsOnResolve() {
    SecretCredentialsProvider provider =
        SecretCredentialsProvider.create("NoSuchSecretManager", spec("key"), spec("secret"));
    assertThatThrownBy(provider::resolveCredentials)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("NoSuchSecretManager")
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testToStringDoesNotContainCredentials() {
    SecretCredentialsProvider provider =
        SecretCredentialsProvider.create(NAME, spec("my-key"), spec("my-secret"));
    provider.resolveCredentials();
    assertThat(provider.toString()).doesNotContain("my-key").doesNotContain("my-secret");
  }
}
