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
        String versionName = paramMap.get("version_name");
        if (versionName == null) {
          throw new RuntimeException(
              "version_name must contain a valid value for versionName parameter");
        }
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
        return new GcpHsmGeneratedSecret(
            paramMap.get("project_id"),
            paramMap.get("location_id"),
            paramMap.get("key_ring_id"),
            paramMap.get("key_id"),
            paramMap.get("job_name"));
      default:
        throw new RuntimeException(
            String.format(
                "Invalid secret type %s, currently only GcpSecret and GcpHsmGeneratedSecret are supported",
                secretType));
    }
  }
}
