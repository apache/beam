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

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/** @deprecated Use {@link ProtoBeamConverter} */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
@Deprecated
public class ProtoDynamicMessageSchema<T> implements Serializable {
  public static final long serialVersionUID = 1L;

  private final Schema schema;
  private final SerializableFunction<Row, Message> toProto;
  private final SerializableFunction<Message, Row> fromProto;

  private ProtoDynamicMessageSchema(Descriptors.Descriptor descriptor, Schema schema) {
    this.schema = schema;
    this.toProto = ProtoBeamConverter.toProto(descriptor);
    this.fromProto = ProtoBeamConverter.toRow(schema);
  }

  /**
   * Create a new ProtoDynamicMessageSchema from a {@link ProtoDomain} and for a message. The
   * message need to be in the domain and needs to be the fully qualified name.
   */
  public static ProtoDynamicMessageSchema forDescriptor(ProtoDomain domain, String messageName) {
    Descriptors.Descriptor descriptor = domain.getDescriptor(messageName);
    Schema schema = ProtoSchemaTranslator.getSchema(descriptor);
    return new ProtoDynamicMessageSchema(descriptor, schema);
  }

  /**
   * Create a new ProtoDynamicMessageSchema from a {@link ProtoDomain} and for a descriptor. The
   * descriptor is only used for it's name, that name will be used for a search in the domain.
   */
  public static ProtoDynamicMessageSchema<DynamicMessage> forDescriptor(
      ProtoDomain domain, Descriptors.Descriptor descriptor) {
    return forDescriptor(domain, descriptor.getFullName());
  }

  public Schema getSchema() {
    return schema;
  }

  public SerializableFunction<T, Row> getToRowFunction() {
    return message -> {
      Message message2 = (Message) message;
      return fromProto.apply(Preconditions.checkNotNull(message2));
    };
  }

  @SuppressWarnings("unchecked")
  public SerializableFunction<Row, T> getFromRowFunction() {
    return row -> (T) toProto.apply(row);
  }
}
