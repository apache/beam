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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for working with release information.
 */
public final class ReleaseInfo extends GenericJson {
  private static final Logger LOG = LoggerFactory.getLogger(ReleaseInfo.class);

  private static final String MANIFEST_TIMESTAMP_ATTRIBUTE = "Build-timestamp";

  private static class LazyInit {
    private static final ReleaseInfo INSTANCE = new ReleaseInfo();
  }

  /**
   * The string that will be used as a version if none can be ascertained.
   */
  public static final String UNKNOWN_VERSION_STRING = "Unknown";

  /**
   * Returns an instance of {@link ReleaseInfo}.
   */
  public static ReleaseInfo getReleaseInfo() {
    return LazyInit.INSTANCE;
  }

  @Key private String name = "Apache Beam SDK for Java";
  @Key private String version = UNKNOWN_VERSION_STRING;
  @Nullable @Key("build.date") private String timestamp = null;

  /** Provides the SDK name. */
  public String getName() {
    return name;
  }

  /**
   * The version of the packaged artifact for the core Apache Beam Java SDK, or
   * {@link #UNKNOWN_VERSION_STRING} if unknown.
   *
   * <p>This should only be unknown if the class is used outside of a built artifact.
   */
  public String getVersion() {
    return version;
  }

  /**
   * The build time of the packaged artifact for the core Apache Beam Java SDK, or {@code null} if
   * unknown.
   *
   * <p>This should only be {@code null} if the class is used outside of a built artifact.
   */
  @Nullable
  public String getBuildTimestamp() {
    return timestamp;
  }

  private ReleaseInfo() {
    // This may not actually be a jar; in that case we cannot learn anything from the manifest
    URL jarLocation = getClass().getProtectionDomain().getCodeSource().getLocation();

    // Get the version via standard channels
    @Nullable String maybeVersion = getClass().getPackage().getImplementationVersion();
    if (maybeVersion != null) {
      version = maybeVersion;
    }

    // Read the timestamp, which is a custom manifest entry
    Manifest manifest;
    try (JarFile jarFile = new JarFile(jarLocation.toURI().getPath())) {
      manifest = jarFile.getManifest();
    } catch (IOException exc) {
      LOG.warn("Beam SDK jar manifest not found at " + jarLocation, exc);
      return;
    } catch (URISyntaxException exc) {
      LOG.warn("Beam SDK code location appears to be invalid at " + jarLocation, exc);
      return;
    }
    timestamp = manifest.getMainAttributes().getValue(MANIFEST_TIMESTAMP_ATTRIBUTE);
  }
}
