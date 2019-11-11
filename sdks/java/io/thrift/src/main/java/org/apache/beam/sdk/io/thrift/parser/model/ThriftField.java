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

import static org.apache.beam.sdk.io.thrift.codec.ThriftField.RECURSIVE_REFERENCE_ANNOTATION_NAME;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.thrift.parser.visitor.DocumentVisitor;
import org.apache.beam.sdk.io.thrift.parser.visitor.Visitable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public class ThriftField implements Visitable, Serializable {
  public enum Requiredness {
    REQUIRED,
    OPTIONAL,
    NONE
  }

  private final String name;
  private final ThriftType type;
  private final Optional<Long> identifier;
  private final Requiredness requiredness;
  private final Optional<ConstValue> value;
  private final List<TypeAnnotation> annotations;
  private final boolean isRecursiveReference;

  public ThriftField(
      String name,
      ThriftType type,
      Long identifier,
      Requiredness requiredness,
      ConstValue value,
      List<TypeAnnotation> annotations) {
    this.name = checkNotNull(name, "name");
    this.type = checkNotNull(type, "type");
    this.identifier = Optional.fromNullable(identifier);
    this.requiredness = checkNotNull(requiredness, "requiredness");
    this.value = Optional.fromNullable(value);
    this.annotations = checkNotNull(annotations, "annotations");

    // Convert swift.recursive_reference annotations to isRecursive, and drop them
    this.isRecursiveReference =
        Iterables.removeIf(
            annotations,
            annotation ->
                annotation.getName().equals(RECURSIVE_REFERENCE_ANNOTATION_NAME)
                    && annotation.getValue().equals("true"));
  }

  public String getName() {
    return name;
  }

  public ThriftType getType() {
    return type;
  }

  public Optional<Long> getIdentifier() {
    return identifier;
  }

  public Requiredness getRequiredness() {
    return requiredness;
  }

  public boolean isRecursive() {
    return isRecursiveReference;
  }

  public Optional<ConstValue> getValue() {
    return value;
  }

  public List<TypeAnnotation> getAnnotations() {
    return annotations;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("identifier", identifier)
        .add("requiredness", requiredness)
        .add("value", value)
        .add("annotations", annotations)
        .toString();
  }

  @Override
  public void visit(final DocumentVisitor visitor) throws IOException {
    visitor.visit(this);
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof ThriftField)) {
      return false;
    }

    ThriftField c = (ThriftField) o;

    return Objects.equals(name, c.name)
        && Objects.equals(type, c.type)
        && Objects.equals(identifier, c.identifier)
        && Objects.equals(requiredness, c.requiredness)
        && Objects.equals(isRecursiveReference, c.isRecursiveReference)
        && Objects.equals(value, c.value)
        && Objects.equals(annotations, c.annotations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, type, identifier, requiredness, isRecursiveReference, value, annotations);
  }
}
