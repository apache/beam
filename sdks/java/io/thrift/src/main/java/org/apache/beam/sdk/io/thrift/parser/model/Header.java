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
package org.apache.beam.sdk.io.thrift.parser.model;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

public class Header implements Serializable {
  private List<String> includes;
  private Map<String, String> namespaces;
  private String defaultNamespace;
  private List<String> cppIncludes;

  public Header(
      List<String> includes,
      List<String> cppIncludes,
      String defaultNamespace,
      Map<String, String> namespaces) {
    this.includes = ImmutableList.copyOf(checkNotNull(includes, "includes"));
    this.cppIncludes = ImmutableList.copyOf(checkNotNull(cppIncludes, "cppIncludes"));
    this.defaultNamespace = defaultNamespace;
    this.namespaces = ImmutableMap.copyOf(checkNotNull(namespaces, "namespaces"));
  }

  public List<String> getIncludes() {
    return includes;
  }

  void setIncludes(List<String> includes) {
    this.includes = ImmutableList.copyOf(checkNotNull(includes, "includes"));
  }

  public String getNamespace(final String nameSpaceName) {
    String namespace = namespaces.get(nameSpaceName);
    if (namespace == null) {
      namespace = defaultNamespace;
    }
    return namespace;
  }

  public Map<String, String> getNamespaces() {
    return namespaces;
  }

  void setNamespaces(Map<String, String> namespaces) {
    this.namespaces = namespaces;
  }

  public List<String> getCppIncludes() {
    return cppIncludes;
  }

  void setCppIncludes(List<String> cppIncludes) {
    this.cppIncludes = ImmutableList.copyOf(checkNotNull(cppIncludes, "cppIncludes"));
  }

  String getDefaultNamespace() {
    return defaultNamespace;
  }

  void setDefaultNamespace(String defaultNamespace) {
    this.defaultNamespace = defaultNamespace;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("includes", includes)
        .add("cppIncludes", cppIncludes)
        .add("defaultNamespace", defaultNamespace)
        .add("namespaces", namespaces)
        .toString();
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof Header)) {
      return false;
    }

    Header c = (Header) o;

    return Objects.equals(includes, c.includes)
        && Objects.equals(cppIncludes, c.cppIncludes)
        && Objects.equals(namespaces, c.namespaces)
        && Objects.equals(defaultNamespace, c.defaultNamespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(includes, cppIncludes, namespaces, defaultNamespace);
  }
}
