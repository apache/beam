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
package org.apache.beam.sdk.coders;

import com.google.api.services.datastore.DatastoreV1.Entity;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link Coder} for {@link Entity} objects based on their encoded Protocol Buffer form.
 */
public class EntityCoder extends AtomicCoder<Entity> {

  @JsonCreator
  public static EntityCoder of() {
    return INSTANCE;
  }

  /***************************/

  private static final EntityCoder INSTANCE = new EntityCoder();

  private EntityCoder() {}

  @Override
  public void encode(Entity value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null Entity");
    }

    // Since Entity implements com.google.protobuf.MessageLite,
    // we could directly use writeTo to write to a OutputStream object
    outStream.write(java.nio.ByteBuffer.allocate(4).putInt(value.getSerializedSize()).array());
    value.writeTo(outStream);
    outStream.flush();
  }

  @Override
  public Entity decode(InputStream inStream, Context context)
      throws IOException {
    byte[] entitySize = new byte[4];
    inStream.read(entitySize, 0, 4);
    int size = java.nio.ByteBuffer.wrap(entitySize).getInt();
    byte[] data = new byte[size];
    inStream.read(data, 0, size);
    return Entity.parseFrom(data);
  }

  @Override
  protected long getEncodedElementByteSize(Entity value, Context context)
      throws Exception {
    return value.getSerializedSize();
  }

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException always.
   *         A datastore kind can hold arbitrary {@link Object} instances, which
   *         makes the encoding non-deterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "Datastore encodings can hold arbitrary Object instances");
  }
}
