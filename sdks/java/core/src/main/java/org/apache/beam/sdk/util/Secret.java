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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * A secret management interface used for handling sensitive data.
 *
 * <p>This interface provides a generic way to handle secrets. Implementations of this interface
 * should handle fetching secrets from a secret management system. The underlying secret management
 * system should be able to return a valid byte array representing the secret.
 */
public interface Secret extends Serializable {
  /**
   * Returns the secret as a byte array.
   *
   * @return The secret as a byte array.
   */
  byte[] getSecretBytes();

  static Secret parseSecretOption(String secretOption) {
    Map<String, String> paramMap = new HashMap<>();
    for (String param : secretOption.split(";", -1)) {
      String[] parts = param.split(":", 2);
      if (parts.length == 2) {
        paramMap.put(parts[0], parts[1]);
      }
    }

    if (!paramMap.containsKey("type")) {
      throw new RuntimeException("Secret string must contain a valid type parameter");
    }

    String secretType = paramMap.get("type");
    paramMap.remove("type");

    if (secretType == null) {
      throw new RuntimeException("Secret string must contain a valid value for type parameter");
    }

    switch (secretType.toLowerCase()) {
      case "gcpsecret":
        Set<String> gcpSecretParams = new HashSet<>(Arrays.asList("version_name"));
        for (String paramName : paramMap.keySet()) {
          if (!gcpSecretParams.contains(paramName)) {
            throw new RuntimeException(
                String.format(
                    "Invalid secret parameter %s, GcpSecret only supports the following parameters: %s",
                    paramName, gcpSecretParams));
          }
        }
        String versionName =
            Preconditions.checkNotNull(
                paramMap.get("version_name"),
                "version_name must contain a valid value for versionName parameter");
        return new GcpSecret(versionName);
      case "gcphsmgeneratedsecret":
        Set<String> gcpHsmGeneratedSecretParams =
            new HashSet<>(
                Arrays.asList("project_id", "location_id", "key_ring_id", "key_id", "job_name"));
        for (String paramName : paramMap.keySet()) {
          if (!gcpHsmGeneratedSecretParams.contains(paramName)) {
            throw new RuntimeException(
                String.format(
                    "Invalid secret parameter %s, GcpHsmGeneratedSecret only supports the following parameters: %s",
                    paramName, gcpHsmGeneratedSecretParams));
          }
        }
        String projectId =
            Preconditions.checkNotNull(
                paramMap.get("project_id"),
                "project_id must contain a valid value for projectId parameter");
        String locationId =
            Preconditions.checkNotNull(
                paramMap.get("location_id"),
                "location_id must contain a valid value for locationId parameter");
        String keyRingId =
            Preconditions.checkNotNull(
                paramMap.get("key_ring_id"),
                "key_ring_id must contain a valid value for keyRingId parameter");
        String keyId =
            Preconditions.checkNotNull(
                paramMap.get("key_id"), "key_id must contain a valid value for keyId parameter");
        String jobName =
            Preconditions.checkNotNull(
                paramMap.get("job_name"),
                "job_name must contain a valid value for jobName parameter");
        return new GcpHsmGeneratedSecret(projectId, locationId, keyRingId, keyId, jobName);
      default:
        throw new RuntimeException(
            String.format(
                "Invalid secret type %s, currently only GcpSecret and GcpHsmGeneratedSecret are supported",
                secretType));
    }
  }
}
