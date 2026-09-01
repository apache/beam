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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A secret management class used for handling sensitive data.
 *
 * <p>This class provides a generic way to handle secrets. Implementations of this class should
 * handle fetching secrets from a secret management system. The underlying secret management system
 * should be able to return a valid byte array representing the secret.
 */
public abstract class Secret implements Serializable {
  private transient byte @Nullable [] cachedSecretBytes = null;

  /**
   * Returns the secret as a byte array.
   *
   * @return The secret as a byte array.
   */
  public abstract byte[] getSecretBytes();

  /** Returns the secret as a byte array, optionally caching the result in memory. */
  public synchronized byte[] getBytes(boolean cacheSecret) {
    byte[] localCached = cachedSecretBytes;
    if (cacheSecret && localCached != null) {
      return localCached;
    }
    byte[] secretBytes = getSecretBytes();
    if (cacheSecret) {
      this.cachedSecretBytes = secretBytes;
    }
    return secretBytes;
  }

  /** Returns the secret as a byte array without caching. */
  public byte[] getBytes() {
    return getBytes(false);
  }

  /** Returns secret value as UTF-8 string, optionally caching the result in memory. */
  public @Nullable String getString(boolean cacheSecret) {
    byte[] secretBytes = getBytes(cacheSecret);
    return secretBytes == null ? null : new String(secretBytes, StandardCharsets.UTF_8);
  }

  /** Returns secret value as UTF-8 string without caching. */
  public @Nullable String getString() {
    return getString(false);
  }

  /**
   * Parses a secret string and returns the appropriate secret type.
   *
   * <p>The secret string should be formatted like:
   * 'type:&lt;secret_type&gt;;&lt;secret_param&gt;:&lt;value&gt;'
   *
   * <p>For example, 'type:GcpSecret;version_name:my_secret/versions/latest' would return a
   * GcpSecret initialized with 'my_secret/versions/latest'.
   */
  public static Secret parseSecretOption(String secretOption) {
    if (secretOption == null) {
      throw new IllegalArgumentException("Secret option string cannot be null");
    }
    Map<String, String> paramMap = new HashMap<>();
    for (String param : secretOption.split(";", -1)) {
      String[] parts = param.split(":", 2);
      if (parts.length == 2) {
        paramMap.put(parts[0], parts[1]);
      }
    }

    if (!paramMap.containsKey("type")) {
      throw new IllegalArgumentException("Secret string must contain a valid type parameter");
    }

    String rawType = paramMap.remove("type");
    if (rawType == null || rawType.isEmpty()) {
      throw new IllegalArgumentException("Secret string must contain a valid type parameter");
    }

    String secretType = rawType.toLowerCase();
    String secretManager;
    switch (secretType) {
      case "gcpsecret":
        secretManager = "GoogleCloudSecretManager";
        break;
      case "gcphsmgeneratedsecret":
        secretManager = "GoogleCloudHsmGeneratedSecretManager";
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Invalid secret type %s, currently only GcpSecret and GcpHsmGeneratedSecret are supported",
                secretType));
    }

    try {
      ObjectMapper mapper = new ObjectMapper();
      String jsonSpec = mapper.writeValueAsString(paramMap);
      return fromJson(jsonSpec, secretManager);
    } catch (Exception e) {
      if (e instanceof IllegalArgumentException) {
        throw (IllegalArgumentException) e;
      }
      throw new RuntimeException("Failed to parse secret option", e);
    }
  }

  /**
   * Return a Secret instance based on secret_manager provider and secret specification JSON string.
   *
   * @param spec Secret string (raw secret or JSON specification string).
   * @param secretManager Secret manager string (e.g. 'GoogleCloudSecretManager').
   * @return An instance of Secret.
   */
  public static Secret fromJson(@Nullable String spec, @Nullable String secretManager) {
    Logger logger = LoggerFactory.getLogger(Secret.class);
    String smManager = secretManager != null ? secretManager.trim() : null;
    if (smManager != null && smManager.isEmpty()) {
      smManager = null;
    }

    Map<String, String> specMap = null;
    if (spec != null && !spec.isEmpty()) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        specMap = mapper.readValue(spec, new TypeReference<Map<String, String>>() {});
      } catch (Exception e) {
        logger.debug("Failed to parse secret spec as JSON map", e);
      }
    }

    if (smManager != null) {
      switch (smManager.toLowerCase()) {
        case "googlecloudsecretmanager":
        case "gcpsecret":
          if (specMap != null) {
            return GcpSecret.fromMap(specMap);
          } else if (spec != null) {
            return new GcpSecret(spec);
          } else {
            throw new IllegalArgumentException("Invalid spec for GcpSecret");
          }
        case "googlecloudhsmgeneratedsecretmanager":
        case "gcphsmgeneratedsecret":
          if (specMap != null) {
            return GcpHsmGeneratedSecret.fromMap(specMap);
          } else {
            throw new IllegalArgumentException("Invalid spec for GcpHsmGeneratedSecret");
          }
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Unsupported secret manager: '%s'. Currently supported options: 'GoogleCloudSecretManager', 'GoogleCloudHsmGeneratedSecretManager'.",
                  smManager));
      }
    }

    if (specMap != null) {
      logger.warn(
          "The 'spec' parameter appears to be a JSON specification, but 'secret_manager' is not set. Defaulting to Raw.");
    }

    return new RawSecret(spec != null ? spec : "");
  }

  /**
   * Return a Secret instance with default raw secret handling.
   *
   * @param spec Secret string (raw secret or JSON specification string).
   * @return An instance of Secret.
   */
  public static Secret fromJson(@Nullable String spec) {
    return fromJson(spec, null);
  }
}
