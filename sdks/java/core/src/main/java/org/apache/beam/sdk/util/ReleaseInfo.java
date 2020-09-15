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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Properties pertaining to this release of Apache Beam.
 *
 * <p>Properties will always include a name and version.
 */
@AutoValue
@Internal
public abstract class ReleaseInfo implements Serializable {
  private static final String PROPERTIES_PATH = "/org/apache/beam/sdk/sdk.properties";

  /** Returns an instance of {@link ReleaseInfo}. */
  public static ReleaseInfo getReleaseInfo() {
    return LazyInit.INSTANCE;
  }

  /** Returns an immutable map of all properties pertaining to this release. */
  public abstract Map<String, String> getProperties();

  /** Provides the SDK name. */
  public String getName() {
    return getProperties().get("name");
  }

  /** Provides the BEAM version. ie: 2.18.0-SNAPSHOT */
  public String getVersion() {
    return getProperties().get("version");
  }

  /** Provides the SDK version. ie: 2.18.0 or 2.18.0.dev */
  public String getSdkVersion() {
    return getProperties().get("sdk_version");
  }

  /** Provides docker image default root (apache). */
  public String getDefaultDockerRepoRoot() {
    return getProperties().get("docker_image_default_repo_root");
  }

  /** Provides docker image default repo prefix (beam_). */
  public String getDefaultDockerRepoPrefix() {
    return getProperties().get("docker_image_default_repo_prefix");
  }

  /////////////////////////////////////////////////////////////////////////
  private static final Logger LOG = LoggerFactory.getLogger(ReleaseInfo.class);
  private static final String DEFAULT_NAME = "Apache Beam SDK for Java";
  private static final String DEFAULT_VERSION = "Unknown";
  private static final String DEFAULT_DOCKER_IMAGE_ROOT = "apache";
  private static final String DEFAULT_DOCKER_IMAGE_PREFIX = "beam_";

  private static class LazyInit {
    private static final ReleaseInfo INSTANCE;

    static {
      Properties properties = new Properties();
      try (InputStream in = ReleaseInfo.class.getResourceAsStream(PROPERTIES_PATH)) {
        if (in == null) {
          LOG.warn("Beam properties resource not found: {}", PROPERTIES_PATH);
        } else {
          properties.load(in);
        }
      } catch (IOException e) {
        LOG.warn("Error loading Beam properties resource: ", e);
      }
      if (!properties.containsKey("name")) {
        properties.setProperty("name", DEFAULT_NAME);
      }
      if (!properties.containsKey("version")) {
        properties.setProperty("version", DEFAULT_VERSION);
      }
      if (!properties.containsKey("sdk_version")) {
        properties.setProperty("sdk_version", DEFAULT_VERSION);
      }
      if (!properties.containsKey("docker_image_default_repo_root")) {
        properties.setProperty("docker_image_default_repo_root", DEFAULT_DOCKER_IMAGE_ROOT);
      }
      if (!properties.containsKey("docker_image_default_repo_prefix")) {
        properties.setProperty("docker_image_default_repo_prefix", DEFAULT_DOCKER_IMAGE_PREFIX);
      }
      INSTANCE = new AutoValue_ReleaseInfo(ImmutableMap.copyOf((Map) properties));
    }
  }
}
