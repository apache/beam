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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.thrift.parser.visitor.DocumentVisitor;
import org.apache.beam.sdk.io.thrift.parser.visitor.Visitable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class Service extends Definition {
  private final String name;
  private final Optional<String> parent;
  private final List<ThriftMethod> methods;
  private final Optional<List<TypeAnnotation>> annotations;

  public Service(
      String name, String parent, List<ThriftMethod> methods, List<TypeAnnotation> annotations) {
    this.name = checkNotNull(name, "name");
    this.parent = Optional.fromNullable(parent);
    this.annotations = Optional.fromNullable(annotations);
    this.methods = ImmutableList.copyOf(checkNotNull(methods, "methods"));
  }

  @Override
  public String getName() {
    return name;
  }

  public Optional<String> getParent() {
    return parent;
  }

  public List<ThriftMethod> getMethods() {
    return methods;
  }

  public Optional<List<TypeAnnotation>> getAnnotations() {
    return annotations;
  }

  @Override
  public void visit(final DocumentVisitor visitor) throws IOException {
    super.visit(visitor);
    Visitable.Utils.visitAll(visitor, getMethods());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("parent", parent)
        .add("methods", methods)
        .toString();
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof Service)) {
      return false;
    }

    Service c = (Service) o;

    return Objects.equals(name, c.name)
        && Objects.equals(parent, c.parent)
        && Objects.equals(methods, c.methods)
        && Objects.equals(annotations, c.annotations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, parent, methods, annotations);
  }
}
