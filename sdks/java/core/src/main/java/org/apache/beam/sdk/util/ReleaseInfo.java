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

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for working with release information.
 */
public final class ReleaseInfo extends GenericJson {
  private static final Logger LOG = LoggerFactory.getLogger(ReleaseInfo.class);

  private static final String PROPERTIES_PATH =
      "/org/apache/beam/sdk/sdk.properties";

  private static class LazyInit {
    private static final ReleaseInfo INSTANCE =
        new ReleaseInfo(PROPERTIES_PATH);
  }

  /**
   * Returns an instance of {@link ReleaseInfo}.
   */
  public static ReleaseInfo getReleaseInfo() {
    return LazyInit.INSTANCE;
  }

  @Key private String name = "Apache Beam SDK for Java";
  @Key private String version = "Unknown";

  /** Provides the SDK name. */
  public String getName() {
    return name;
  }

  /** Provides the SDK version. */
  public String getVersion() {
    return version;
  }

  private ReleaseInfo(String resourcePath) {
    Properties properties = new Properties();

    try (InputStream in = ReleaseInfo.class.getResourceAsStream(PROPERTIES_PATH)) {
      if (in == null) {
        LOG.warn("Beam properties resource not found: {}", resourcePath);
        return;
      }

      properties.load(in);
    } catch (IOException e) {
      LOG.warn("Error loading Beam properties resource: ", e);
    }

    for (String name : properties.stringPropertyNames()) {
      if (name.equals("name")) {
        // We don't allow the properties to override the SDK name.
        continue;
      }
      put(name, properties.getProperty(name));
    }
  }
}
