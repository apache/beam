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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link org.apache.beam.sdk.util.Secret}. */
@RunWith(JUnit4.class)
public class SecretTest {

  @Test
  public void testParseSecretOptionWithMissingType() {
    String secretOption = "version_name:my_secret/versions/latest";
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> Secret.parseSecretOption(secretOption));
    assertEquals("Secret string must contain a valid type parameter", exception.getMessage());
  }

  @Test
  public void testParseSecretOptionWithUnsupportedType() {
    String secretOption = "type:unsupported;version_name:my_secret/versions/latest";
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> Secret.parseSecretOption(secretOption));
    assertTrue(exception.getMessage().contains("Invalid secret type unsupported"));
  }

  @Test
  public void testRawSecretStr() {
    Secret secret = new RawSecret("STATIC_SECRET_");
    assertEquals("STATIC_SECRET_", secret.getString(true));
    assertEquals("STATIC_SECRET_", secret.getString(false));
    assertEquals("STATIC_SECRET_", secret.getString());
    assertArrayEquals("STATIC_SECRET_".getBytes(StandardCharsets.UTF_8), secret.getBytes(true));
    assertArrayEquals("STATIC_SECRET_".getBytes(StandardCharsets.UTF_8), secret.getBytes());
  }

  @Test
  public void testRawSecretBytes() {
    byte[] bytes = "STATIC_BYTES_".getBytes(StandardCharsets.UTF_8);
    Secret secret = new RawSecret(bytes);
    assertEquals("STATIC_BYTES_", secret.getString(true));
    assertEquals("STATIC_BYTES_", secret.getString());
    assertArrayEquals(bytes, secret.getBytes(true));
    assertArrayEquals(bytes, secret.getBytes());
  }

  @Test
  public void testSecretFactory() {
    Secret secretRaw = Secret.fromJson("STATIC_SECRET_", null);
    assertTrue(secretRaw instanceof RawSecret);
    assertEquals("STATIC_SECRET_", secretRaw.getString());

    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> Secret.fromJson("spec", "unsupported_provider"));
    assertTrue(
        exception.getMessage().contains("Unsupported secret manager: 'unsupported_provider'"));
  }

  @Test
  public void testJsonSecretWithoutSecretManagerWarning() {
    String jsonSpec = "{\"name\": \"my-secret\", \"project\": \"my-proj\"}";
    Secret secret = Secret.fromJson(jsonSpec, null);
    assertTrue(secret instanceof RawSecret);
  }

  @Test
  public void testEquality() {
    RawSecret raw1 = new RawSecret("secret_value");
    RawSecret raw2 = new RawSecret("secret_value");
    RawSecret raw3 = new RawSecret("other_value");
    assertEquals(raw1, raw2);
    assertEquals(raw1.hashCode(), raw2.hashCode());
    assertNotEquals(raw1, raw3);
    assertFalse(raw1.equals("secret_value"));
    assertFalse(raw1.equals(null));
  }

  @Test
  public void testSerialization() {
    RawSecret raw = new RawSecret("test_secret");
    RawSecret deserializedRaw = SerializableUtils.clone(raw);
    assertEquals(raw, deserializedRaw);
  }

  @Test
  public void testLoadSecretFactoriesNullList() {
    Map<String, SecretRegistrar.SecretFactory> factories = Secret.loadSecretFactories(null);
    assertTrue(factories.isEmpty());
  }

  @Test
  public void testLoadSecretFactoriesHandlesNullRegistrarAndNullFactories() {
    SecretRegistrar nullFactoriesRegistrar = () -> null;
    Map<String, SecretRegistrar.SecretFactory> factories =
        Secret.loadSecretFactories(java.util.Arrays.asList(null, nullFactoriesRegistrar));
    assertTrue(factories.isEmpty());
  }

  @Test
  public void testLoadSecretFactoriesHandlesThrowingRegistrar() {
    SecretRegistrar throwingRegistrar =
        () -> {
          throw new RuntimeException("Simulated failure in registrar");
        };
    SecretRegistrar validRegistrar =
        () -> Collections.singletonMap("valid", spec -> new RawSecret("test"));

    Map<String, SecretRegistrar.SecretFactory> factories =
        Secret.loadSecretFactories(java.util.Arrays.asList(throwingRegistrar, validRegistrar));
    assertEquals(1, factories.size());
    assertTrue(factories.containsKey("valid"));
  }

  @Test
  public void testLoadSecretFactoriesHandlesMalformedEntries() {
    Map<String, SecretRegistrar.SecretFactory> malformedMap = new HashMap<>();
    malformedMap.put(null, spec -> new RawSecret("val"));
    malformedMap.put("", spec -> new RawSecret("val"));
    malformedMap.put("   ", spec -> new RawSecret("val"));
    malformedMap.put("null_factory", null);
    malformedMap.put("good", spec -> new RawSecret("good_val"));

    SecretRegistrar registrar = () -> malformedMap;
    Map<String, SecretRegistrar.SecretFactory> factories =
        Secret.loadSecretFactories(Collections.singletonList(registrar));
    assertEquals(1, factories.size());
    assertTrue(factories.containsKey("good"));
  }

  @Test
  public void testLoadSecretFactoriesDuplicateKeysFirstWins() {
    SecretRegistrar.SecretFactory factory1 = spec -> new RawSecret("first");
    SecretRegistrar.SecretFactory factory2 = spec -> new RawSecret("second");

    SecretRegistrar registrar1 = () -> Collections.singletonMap("duplicate_key", factory1);
    SecretRegistrar registrar2 = () -> Collections.singletonMap("DUPLICATE_KEY", factory2);

    Set<String> supportedTypes = new TreeSet<>();
    Map<String, SecretRegistrar.SecretFactory> factories =
        Secret.loadSecretFactories(java.util.Arrays.asList(registrar1, registrar2), supportedTypes);
    assertEquals(1, factories.size());
    assertEquals(factory1, factories.get("duplicate_key"));
    assertEquals(Collections.singleton("duplicate_key"), supportedTypes);
  }
}
