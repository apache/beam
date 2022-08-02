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
package org.apache.beam.sdk.util;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;

/**
 * A {@link Serializable} {@link ThreadLocal} which discards any "stored" objects. This allows for
 * Kryo to serialize a {@link Coder} as a final field.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class EmptyOnDeserializationThreadLocal<T> extends ThreadLocal<T> implements Serializable {
  private void writeObject(java.io.ObjectOutputStream out) throws IOException {}

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {}

  private void readObjectNoData() throws ObjectStreamException {}
}
