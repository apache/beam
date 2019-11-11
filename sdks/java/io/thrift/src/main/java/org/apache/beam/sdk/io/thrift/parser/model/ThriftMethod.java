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
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.thrift.parser.visitor.DocumentVisitor;
import org.apache.beam.sdk.io.thrift.parser.visitor.Visitable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class ThriftMethod implements Visitable, Serializable {
  private final String name;
  private final ThriftType returnType;
  private final List<ThriftField> arguments;
  private final boolean oneway;
  private final List<ThriftField> throwsFields;
  private final List<TypeAnnotation> annotations;

  public ThriftMethod(
      String name,
      ThriftType returnType,
      List<ThriftField> arguments,
      boolean oneway,
      List<ThriftField> throwsFields,
      List<TypeAnnotation> annotations) {
    this.name = checkNotNull(name, "name");
    this.returnType = checkNotNull(returnType, "returnType");
    this.arguments = ImmutableList.copyOf(checkNotNull(arguments, "arguments"));
    this.oneway = oneway;
    this.throwsFields =
        ImmutableList.copyOf(
            MoreObjects.firstNonNull(throwsFields, ImmutableList.<ThriftField>of()));
    this.annotations = ImmutableList.copyOf(checkNotNull(annotations, "annotations"));
  }

  public String getName() {
    return name;
  }

  public ThriftType getReturnType() {
    return returnType;
  }

  public List<ThriftField> getArguments() {
    return arguments;
  }

  public boolean isOneway() {
    return oneway;
  }

  public List<ThriftField> getThrowsFields() {
    return throwsFields;
  }

  public List<TypeAnnotation> getAnnotations() {
    return annotations;
  }

  @Override
  public void visit(final DocumentVisitor visitor) throws IOException {
    Visitable.Utils.visit(visitor, this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("returnType", returnType)
        .add("arguments", arguments)
        .add("oneway", oneway)
        .add("throwsFields", throwsFields)
        .add("annotations", annotations)
        .toString();
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof ThriftMethod)) {
      return false;
    }

    ThriftMethod c = (ThriftMethod) o;

    return Objects.equals(name, c.name)
        && Objects.equals(returnType, c.returnType)
        && Objects.equals(arguments, c.arguments)
        && Objects.equals(oneway, c.oneway)
        && Objects.equals(throwsFields, c.throwsFields)
        && Objects.equals(annotations, c.annotations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, returnType, arguments, oneway, throwsFields, annotations);
  }
}
