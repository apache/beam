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
package org.apache.beam.sdk.schemas.logicaltypes;

import org.apache.beam.sdk.schemas.Schema.FieldType;

/**
 * A base class for logical types that are not understood by the Java SDK.
 *
 * <p>Unknown logical types are passed through and treated like their Base type in the Java SDK.
 *
 * <p>Java transforms and JVM runners should take care when processing these types as they may have
 * a particular semantic meaning in the context that created them. For example, consider an
 * enumerated type backed by a primitive {@class FieldType.INT8}. A Java transform can clearly pass
 * through this value and pass it back to a context that understands it, but that transform should
 * not blindly perform arithmetic on this type.
 */
public class UnknownLogicalType<T> extends PassThroughLogicalType<T> {
  private byte[] payload;

  public UnknownLogicalType(
      String identifier,
      byte[] payload,
      FieldType argumentType,
      Object argument,
      FieldType fieldType) {
    super(identifier, argumentType, argument, fieldType);
    this.payload = payload;
  }

  public byte[] getPayload() {
    return payload;
  }
}
