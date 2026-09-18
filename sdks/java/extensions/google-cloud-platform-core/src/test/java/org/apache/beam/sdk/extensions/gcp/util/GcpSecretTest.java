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
package org.apache.beam.sdk.extensions.gcp.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.util.RawSecret;
import org.apache.beam.sdk.util.Secret;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcpSecret} and {@link GcpHsmGeneratedSecret}. */
@RunWith(JUnit4.class)
public class GcpSecretTest {

  @Test
  public void testParseSecretOptionWithValidGcpSecret() {
    String secretOption = "type:gcpsecret;version_name:my_secret/versions/latest";
    Secret secret = Secret.parseSecretOption(secretOption);
    assertTrue(secret instanceof GcpSecret);
    assertEquals("my_secret/versions/latest", ((GcpSecret) secret).getVersionName());
    assertEquals(new GcpSecret("my_secret/versions/latest"), secret);

    Secret secretFoo = Secret.parseSecretOption("type:GcpSecret;version_name:foo");
    assertEquals(new GcpSecret("foo"), secretFoo);

    Secret secretMixedCase =
        Secret.parseSecretOption("type:gcpsecreT;version_name:my_secret/versions/latest");
    assertEquals(new GcpSecret("my_secret/versions/latest"), secretMixedCase);
  }

  @Test
  public void testParseSecretOptionWithValidGcpHsmGeneratedSecret() {
    String secretOption =
        "type:gcphsmgeneratedsecret;project_id:my-project;location_id:global;key_ring_id:my-key-ring;key_id:my-key;job_name:my-job";
    Secret secret = Secret.parseSecretOption(secretOption);
    assertTrue(secret instanceof GcpHsmGeneratedSecret);
    GcpHsmGeneratedSecret hsmSecret = (GcpHsmGeneratedSecret) secret;
    assertEquals("my-project", hsmSecret.getProjectId());
    assertEquals("global", hsmSecret.getLocationId());
    assertEquals("my-key-ring", hsmSecret.getKeyRingId());
    assertEquals("my-key", hsmSecret.getKeyId());
    assertEquals("HsmGeneratedSecret_my-job", hsmSecret.getSecretId());
    assertEquals(
        new GcpHsmGeneratedSecret("my-project", "global", "my-key-ring", "my-key", "my-job"),
        secret);
  }

  @Test
  public void testParseSecretOptionWithInvalidGcpSecretParameter() {
    String secretOption = "type:gcpsecret;invalid_param:some_value";
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> Secret.parseSecretOption(secretOption));
    assertTrue(exception.getMessage().contains("Invalid secret parameter invalid_param"));
  }

  @Test
  public void testParseSecretOptionWithMissingSecretName() {
    String secretOption = "type:gcpsecreT";
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> Secret.parseSecretOption(secretOption));
    assertTrue(exception.getMessage().contains("Secret name must be specified in secret spec."));
  }

  @Test
  public void testSecretFactoryGcp() {
    String spec = "{\"name\": \"test-secret\", \"project\": \"proj\"}";

    Secret secretGcp = Secret.fromJson(spec, "GoogleCloudSecretManager");
    assertTrue(secretGcp instanceof GcpSecret);
    assertEquals(
        "projects/proj/secrets/test-secret/versions/latest",
        ((GcpSecret) secretGcp).getVersionName());

    String singleQuotedSpec = "{'name': 'test-secret', 'project': 'proj'}";
    Secret secretSingleQuoted = Secret.fromJson(singleQuotedSpec, "GoogleCloudSecretManager");
    assertTrue(secretSingleQuoted instanceof GcpSecret);
    assertEquals(
        "projects/proj/secrets/test-secret/versions/latest",
        ((GcpSecret) secretSingleQuoted).getVersionName());

    // Case-insensitive secret manager lookup in fromJson
    Secret secretGcpLower = Secret.fromJson(spec, "googlecloudsecretmanager");
    assertTrue(secretGcpLower instanceof GcpSecret);
    Secret secretShortLower = Secret.fromJson(spec, "gcpsecret");
    assertTrue(secretShortLower instanceof GcpSecret);

    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> Secret.fromJson("spec", "unsupported_provider"));
    assertTrue(
        exception.getMessage().contains("Unsupported secret manager: 'unsupported_provider'"));
    assertTrue(exception.getMessage().contains("GoogleCloudSecretManager"));
    assertTrue(exception.getMessage().contains("GcpSecret"));
  }

  @Test
  public void testParseSecretOptionWithUnsupportedTypeListsGcpSecrets() {
    String secretOption = "type:unsupported;version_name:my_secret/versions/latest";
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> Secret.parseSecretOption(secretOption));
    assertTrue(exception.getMessage().contains("Invalid secret type unsupported"));
    assertTrue(exception.getMessage().contains("GcpSecret"));
    assertTrue(exception.getMessage().contains("GoogleCloudSecretManager"));
  }

  @Test
  public void testLoadServicesOrderedDiscoversSecretRegistrars() {
    Iterable<org.apache.beam.sdk.util.SecretRegistrar> registrars =
        org.apache.beam.sdk.util.common.ReflectHelpers.loadServicesOrdered(
            org.apache.beam.sdk.util.SecretRegistrar.class);
    org.junit.Assert.assertNotNull(registrars);
    boolean foundGcp = false;
    for (org.apache.beam.sdk.util.SecretRegistrar registrar : registrars) {
      if (registrar instanceof GcpSecretRegistrar) {
        foundGcp = true;
        break;
      }
    }
    assertTrue("Expected GcpSecretRegistrar to be discovered", foundGcp);
  }

  @Test
  public void testSecretFactoryHsm() {
    String hsmSpec =
        "{\"project_id\": \"p\", \"location_id\": \"l\", \"key_ring_id\": \"r\", \"key_id\": \"k\", \"job_name\": \"j\"}";
    Secret secretHsm = Secret.fromJson(hsmSpec, "GoogleCloudHsmGeneratedSecretManager");
    assertTrue(secretHsm instanceof GcpHsmGeneratedSecret);
    assertEquals("p", ((GcpHsmGeneratedSecret) secretHsm).getProjectId());
  }

  @Test
  public void testEquality() {
    RawSecret raw1 = new RawSecret("secret_value");

    Map<String, String> spec1 = new HashMap<>();
    spec1.put("name", "sec");
    spec1.put("project", "proj");
    Map<String, String> spec2 = new HashMap<>();
    spec2.put("name", "sec");
    spec2.put("project", "proj");
    Map<String, String> spec3 = new HashMap<>();
    spec3.put("name", "other");
    spec3.put("project", "proj");

    GcpSecret gcp1 = GcpSecret.fromMap(spec1);
    GcpSecret gcp2 = GcpSecret.fromMap(spec2);
    GcpSecret gcp3 = GcpSecret.fromMap(spec3);
    assertEquals(gcp1, gcp2);
    assertEquals(gcp1.hashCode(), gcp2.hashCode());
    assertNotEquals(gcp1, gcp3);
    assertFalse(gcp1.equals(raw1));
    assertFalse(gcp1.equals(null));

    GcpHsmGeneratedSecret hsm1 = new GcpHsmGeneratedSecret("p", "l", "r", "k", "j");
    GcpHsmGeneratedSecret hsm2 = new GcpHsmGeneratedSecret("p", "l", "r", "k", "j");
    GcpHsmGeneratedSecret hsm3 = new GcpHsmGeneratedSecret("p", "l", "r", "k", "other");
    assertEquals(hsm1, hsm2);
    assertEquals(hsm1.hashCode(), hsm2.hashCode());
    assertNotEquals(hsm1, hsm3);
    assertFalse(hsm1.equals(gcp1));
    assertFalse(hsm1.equals(null));
  }

  @Test
  public void testGcpSecretFromMapMissingSecretNameThrows() {
    Map<String, String> spec = Collections.singletonMap("project", "my-project");
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GcpSecret.fromMap(spec));
    assertTrue(exception.getMessage().contains("Secret name must be specified"));
  }

  @Test
  public void testGcpHsmGeneratedSecretFromMapMissingParamsThrows() {
    Map<String, String> spec = new HashMap<>();
    spec.put("project_id", "test-proj");
    spec.put("location_id", "global");
    Exception exception =
        assertThrows(NullPointerException.class, () -> GcpHsmGeneratedSecret.fromMap(spec));
    assertTrue(
        exception
            .getMessage()
            .contains("key_ring_id must contain a valid value for keyRingId parameter"));
  }

  @Test
  public void testResolveGcpProjectIdExplicit() {
    assertEquals("my-proj", GcpSecret.resolveGcpProjectId("my-proj", "context"));
  }

  @Test
  public void testSerialization() {
    GcpSecret gcp = new GcpSecret("projects/p/secrets/s/versions/1");
    GcpSecret deserializedGcp = SerializableUtils.clone(gcp);
    assertEquals(gcp, deserializedGcp);

    GcpHsmGeneratedSecret hsm = new GcpHsmGeneratedSecret("p", "l", "r", "k", "j");
    GcpHsmGeneratedSecret deserializedHsm = SerializableUtils.clone(hsm);
    assertEquals(hsm, deserializedHsm);
  }
}
