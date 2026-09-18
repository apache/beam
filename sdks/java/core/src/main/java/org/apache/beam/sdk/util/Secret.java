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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
  private static final Logger LOG = LoggerFactory.getLogger(Secret.class);

  private static final Set<String> SUPPORTED_TYPES;
  private static final Map<String, SecretRegistrar.SecretFactory> SECRET_FACTORIES;

  static {
    TreeSet<String> supportedTypes = new TreeSet<>();
    Map<String, SecretRegistrar.SecretFactory> factories;
    try {
      factories =
          loadSecretFactories(
              ReflectHelpers.loadServicesOrdered(SecretRegistrar.class), supportedTypes);
    } catch (Throwable t) {
      // Top-level fail-safe: guarantee that static class initialization of Secret never fails
      // due to unforeseen classloader or registrar errors.
      LOG.error("Unexpected error loading SecretRegistrars; secret factories may be incomplete", t);
      factories = Collections.emptyMap();
    }
    SECRET_FACTORIES = factories;
    SUPPORTED_TYPES = Collections.unmodifiableSet(supportedTypes);
  }

  /**
   * Loads factories from the provided registrars into an immutable map.
   *
   * <p>Applies defensive checks:
   *
   * <ul>
   *   <li>Sandboxes each registrar with a per-registrar try-catch so a rogue or broken registrar
   *       cannot crash discovery.
   *   <li>Guards against {@code null} return values from {@link
   *       SecretRegistrar#getSecretFactories()}, {@code null} map entries, {@code null} or empty
   *       keys, and {@code null} factory values.
   *   <li>Applies a "first-wins with warning" strategy on duplicate keys to prevent classpath leaks
   *       (such as duplicate test registrars) from throwing exceptions and breaking pipelines.
   * </ul>
   */
  @VisibleForTesting
  static Map<String, SecretRegistrar.SecretFactory> loadSecretFactories(
      @Nullable Iterable<SecretRegistrar> registrars) {
    return loadSecretFactories(registrars, new TreeSet<>());
  }

  @VisibleForTesting
  static Map<String, SecretRegistrar.SecretFactory> loadSecretFactories(
      @Nullable Iterable<SecretRegistrar> registrars, Set<String> supportedTypes) {
    Map<String, SecretRegistrar.SecretFactory> factories = new HashMap<>();
    if (registrars == null) {
      return Collections.emptyMap();
    }

    for (SecretRegistrar registrar : registrars) {
      if (registrar == null) {
        continue;
      }
      try {
        Map<String, SecretRegistrar.SecretFactory> registrarFactories =
            registrar.getSecretFactories();
        if (registrarFactories == null) {
          LOG.warn(
              "SecretRegistrar '{}' returned null from getSecretFactories(); ignoring",
              registrar.getClass().getName());
          continue;
        }

        for (Map.Entry<String, SecretRegistrar.SecretFactory> entry :
            registrarFactories.entrySet()) {
          if (entry == null) {
            continue;
          }
          String rawKey = entry.getKey();
          if (rawKey == null || rawKey.trim().isEmpty()) {
            LOG.warn(
                "SecretRegistrar '{}' registered a factory with a null or empty key; ignoring",
                registrar.getClass().getName());
            continue;
          }
          SecretRegistrar.SecretFactory factory = entry.getValue();
          if (factory == null) {
            LOG.warn(
                "SecretRegistrar '{}' registered a null SecretFactory for key '{}'; ignoring",
                registrar.getClass().getName(),
                rawKey);
            continue;
          }

          String canonicalKey = rawKey.trim();
          String key = canonicalKey.toLowerCase();
          SecretRegistrar.SecretFactory existing = factories.get(key);
          if (existing != null) {
            // First-wins strategy with warning: do not throw to prevent leaked test or duplicate
            // registrars on the classpath from crashing pipeline execution.
            LOG.warn(
                "Duplicate SecretFactory for secret manager name '{}': already registered by '{}', "
                    + "ignoring duplicate from '{}'",
                key,
                existing.getClass().getName(),
                factory.getClass().getName());
          } else {
            factories.put(key, factory);
            supportedTypes.add(canonicalKey);
          }
        }
      } catch (Throwable t) {
        LOG.warn(
            "Failed to load secret factories from SecretRegistrar '{}'; skipping",
            registrar.getClass().getName(),
            t);
      }
    }
    return ImmutableMap.copyOf(factories);
  }

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
    SecretRegistrar.SecretFactory factory = SECRET_FACTORIES.get(secretType);
    if (factory == null) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid secret type %s, currently supported types: %s", rawType, SUPPORTED_TYPES));
    }

    try {
      return factory.createSecret(paramMap);
    } catch (Exception e) {
      if (e instanceof IllegalArgumentException) {
        throw (IllegalArgumentException) e;
      }
      if (e instanceof NullPointerException) {
        throw (NullPointerException) e;
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
        LOG.debug("Failed to parse secret spec as JSON map", e);
      }
    }

    if (smManager != null) {
      SecretRegistrar.SecretFactory factory = SECRET_FACTORIES.get(smManager.toLowerCase());
      if (factory != null) {
        return factory.createSecret(specMap != null ? specMap : Collections.emptyMap());
      }
      throw new IllegalArgumentException(
          String.format(
              "Unsupported secret manager: '%s'. Currently supported options: %s.",
              smManager, SUPPORTED_TYPES));
    }

    if (specMap != null) {
      LOG.warn(
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
