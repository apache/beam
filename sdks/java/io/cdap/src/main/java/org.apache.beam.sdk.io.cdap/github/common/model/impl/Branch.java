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
package org.apache.beam.sdk.io.cdap.github.common.model.impl;

import com.google.api.client.util.Key;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.cdap.github.common.model.GitHubModel;

/** Branch model for github. */
@SuppressWarnings("UnusedVariable")
public class Branch implements GitHubModel {

  @Key private String name;
  @Key private Commit commit;

  @Key("protected")
  private Boolean isProtected;

  @Key private Protection protection;

  @Key("protection_url")
  private String protectionUrl;

  public Branch() {}

  public Branch(String name, Boolean isProtected) {
    this.name = name;
    this.isProtected = isProtected;
  }

  /** Branch.Commit model */
  public static class Commit implements Serializable {
    @Key private String sha;
    @Key private String url;
  }

  /** Branch.Protection model */
  public static class Protection implements Serializable {
    @Key private Boolean enabled;

    @Key("required_status_checks")
    private RequiredStatusChecks requiredStatusChecks;

    /** Branch.Protection.RequiredStatusChecks model */
    public static class RequiredStatusChecks implements Serializable {
      @Key("enforcement_level")
      private String enforcementLevel;

      @Key private List<String> contexts;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Branch branch = (Branch) o;
    return Objects.equals(name, branch.name) && Objects.equals(isProtected, branch.isProtected);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, isProtected);
  }
}
