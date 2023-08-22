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
package org.apache.beam.it.gcp.kms;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import org.apache.beam.it.gcp.GCPBaseIT;
import org.apache.beam.it.gcp.GoogleCloudIntegrationTest;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for KMS Resource Manager. */
@Category(GoogleCloudIntegrationTest.class)
@RunWith(JUnit4.class)
public class KMSResourceManagerIT extends GCPBaseIT {

  private static final String KEYRING_ID = "KMSResourceManagerIT";
  private static final String KEY_ID = "testKey";
  private static final String KMS_REGION = "global";

  private KMSResourceManager kmsResourceManager;

  @Before
  public void setUp() throws IOException {
    kmsResourceManager =
        KMSResourceManager.builder(PROJECT, credentialsProvider).setRegion(KMS_REGION).build();
  }

  @Test
  public void testKMSResourceManagerE2E() {

    String message = RandomStringUtils.randomAlphanumeric(5, 20);

    kmsResourceManager.getOrCreateCryptoKey(KEYRING_ID, KEY_ID);
    String encryptedMessage = kmsResourceManager.encrypt(KEYRING_ID, KEY_ID, message);
    String decryptedMessage = kmsResourceManager.decrypt(KEYRING_ID, KEY_ID, encryptedMessage);

    assertThat(encryptedMessage).isNotEqualTo(message);
    assertThat(decryptedMessage).isEqualTo(message);
  }
}
