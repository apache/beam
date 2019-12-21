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
package org.apache.beam.sdk.extensions.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;

@Experimental(Kind.SCHEMAS)
public abstract class ProtoFieldOverlay<ValueT>
    implements FieldValueGetter<Message, ValueT>, FieldValueSetter<Message.Builder, ValueT> {
  protected final FieldDescriptor fieldDescriptor;
  private final String name;

  public ProtoFieldOverlay(FieldDescriptor fieldDescriptor, String name) {
    this.fieldDescriptor = fieldDescriptor;
    this.name = name;
  }

  @Nullable
  @Override
  public abstract ValueT get(Message object);

  @Override
  public abstract void set(Message.Builder builder, @Nullable ValueT value);

  @Override
  public String name() {
    return name;
  }
}
