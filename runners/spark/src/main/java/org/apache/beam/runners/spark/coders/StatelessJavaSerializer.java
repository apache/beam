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

package org.apache.beam.runners.spark.coders;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.ObjectMap;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


/**
 * Stateless Java serializer.
 * Needed due to a state re-use issue in Kryo version 2.21 used in Spark 1.x
 * See: https://issues.apache.org/jira/browse/SPARK-7708
 * See: https://github.com/EsotericSoftware/kryo/issues/312
 * Copied from Kryo version 2.21.1 com/esotericsoftware/kryo/serializers/JavaSerializer.java
 */
class StatelessJavaSerializer extends Serializer {
  @SuppressWarnings("unchecked")
  public void write (Kryo kryo, Output output, Object object) {
    try {
      ObjectMap graphContext = kryo.getGraphContext();
      ObjectOutputStream objectStream = (ObjectOutputStream) graphContext.get(this);
      if (objectStream == null) {
        objectStream = new ObjectOutputStream(output);
        graphContext.put(this, objectStream);
      }
      objectStream.writeObject(object);
      objectStream.flush();
    } catch (Exception ex) {
      throw new KryoException("Error during Java serialization.", ex);
    }
  }

  @SuppressWarnings("unchecked")
  public Object read (Kryo kryo, Input input, Class type) {
    try {
      ObjectMap graphContext = kryo.getGraphContext();
      ObjectInputStream objectStream = (ObjectInputStream) graphContext.get(this);
      if (objectStream == null) {
        objectStream = new ObjectInputStream(input);
        graphContext.put(this, objectStream);
      }
      return objectStream.readObject();
    } catch (Exception ex) {
      throw new KryoException("Error during Java deserialization.", ex);
    }
  }
}
