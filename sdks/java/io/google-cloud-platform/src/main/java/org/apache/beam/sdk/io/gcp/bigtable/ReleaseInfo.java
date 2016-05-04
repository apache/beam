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
package org.apache.beam.sdk.io.gcp.bigtable;

import javax.annotation.Nullable;

/**
 * Utilities for working with release information.
 */
public final class ReleaseInfo {

  private static final ReleaseInfo INSTANCE = new ReleaseInfo();

  /**
   * Returns the release info of the distributed artifact for the Apache Beam GCP connectors, or
   * {@code null} if unknown.
   */
  public static ReleaseInfo getReleaseInfo() {
    return INSTANCE;
  }

  private ReleaseInfo() { }

  /**
   * The name of the root java package for the Apache Beam GCP connectors.
   */
  public String getName() {
    return ReleaseInfo.class.getPackage().getName();
  }

  /**
   * The version of the distributed artifact for the Apache Beam GCP connectors, or {@code null}
   * if unknown.
   *
   * <p>This should only be {@code null} if the class is used outside of a built artifact.
   */
  @Nullable public String getVersion() {
    return ReleaseInfo.class.getPackage().getImplementationVersion();
  }
}
