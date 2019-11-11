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

import java.io.IOException;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.io.thrift.parser.visitor.DocumentVisitor;
import org.apache.beam.sdk.io.thrift.parser.visitor.Nameable;
import org.apache.beam.sdk.io.thrift.parser.visitor.Visitable;

public abstract class Definition implements Visitable, Nameable, Serializable {
  @Override
  public void visit(final DocumentVisitor visitor) throws IOException {
    Visitable.Utils.visit(visitor, this);
  }

  public Schema getAvroSchema() {
    return ReflectData.get().getSchema(this.getClass());
  }

  @Override
  public abstract String getName();

  @Override
  public abstract String toString();
}
