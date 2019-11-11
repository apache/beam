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

import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public abstract class ContainerType extends ThriftType {
  protected final Optional<String> cppType;
  protected final List<TypeAnnotation> annotations;

  public ContainerType(String cppType, List<TypeAnnotation> annotations) {
    this.cppType = Optional.fromNullable(cppType);
    this.annotations = ImmutableList.copyOf(checkNotNull(annotations, "annotations"));
  }

  public Optional<String> getCppType() {
    return cppType;
  }

  public List<TypeAnnotation> getAnnotations() {
    return annotations;
  }
}
