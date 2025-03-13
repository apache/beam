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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

/**
 * Stateless Java Serializer.
 *
 * <p>Solves state re-use issue in Kryo version 2.21 used in Spark 1.x See:
 * https://issues.apache.org/jira/browse/SPARK-7708
 * https://github.com/EsotericSoftware/kryo/issues/312
 *
 * <p>Also, solves class loading issue in cluster caused by ${@link ObjectInputStream} by using
 * ${@link ObjectInputStreamWithClassLoader} ${@link ObjectInputStream} uses the last user-defined
 * class loader in the stack which can be the wrong class loader. This is a known Java issue and a
 * similar solution is often used. See:
 * https://github.com/apache/spark/blob/v1.6.3/streaming/src/main/scala/org/apache/spark/streaming/Checkpoint.scala#L154
 * https://issues.apache.org/jira/browse/GROOVY-1627
 * https://github.com/spring-projects/spring-loaded/issues/107
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class StatelessJavaSerializer extends Serializer {

  // Since Kryo uses reflection to sequentially look for constructor signatures, starting
  // with this particular signature spares exploring further ones, which involves
  // NoSuchMethodException(s) being thrown as part of the exploration process and may slow
  // things down, see Kryo#newSerializer(), see https://goo.gl/Jn425G
  @SuppressWarnings("unused")
  private StatelessJavaSerializer(final Kryo ignore1, final Class<?> ignore2) {}

  public StatelessJavaSerializer() {
    this(null, null);
  }

  @Override
  public void write(Kryo kryo, Output output, Object object) {
    try {
      ObjectOutputStream objectStream = new ObjectOutputStream(output);
      objectStream.writeObject(object);
      objectStream.flush();
    } catch (Exception e) {
      throw new KryoException("Error during Java serialization.", e);
    }
  }

  @Override
  public Object read(Kryo kryo, Input input, Class type) {
    try {
      return new ObjectInputStreamWithClassLoader(input, kryo.getClassLoader()).readObject();
    } catch (Exception e) {
      throw new KryoException("Error during Java deserialization.", e);
    }
  }

  /** ObjectInputStream with specific ClassLoader. */
  private static class ObjectInputStreamWithClassLoader extends ObjectInputStream {
    private final ClassLoader classLoader;

    ObjectInputStreamWithClassLoader(InputStream in, ClassLoader classLoader) throws IOException {
      super(in);
      this.classLoader = classLoader;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) {
      try {
        return Class.forName(desc.getName(), false, classLoader);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not find class: " + desc.getName(), e);
      }
    }
  }
}
