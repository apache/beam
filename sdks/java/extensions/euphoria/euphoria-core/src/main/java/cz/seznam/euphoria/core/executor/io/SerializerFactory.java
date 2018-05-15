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
package cz.seznam.euphoria.core.executor.io;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/** TODO: complete javadoc. */
@Audience(Audience.Type.EXECUTOR)
public interface SerializerFactory extends Serializable {

  Serializer newSerializer();

  /** TODO: complete javadoc. */
  interface Serializer {

    Output newOutput(OutputStream out);

    Input newInput(InputStream in);

    interface Output extends Closeable {
      void writeObject(Object o);

      void flush();

      @Override
      void close();
    }

    interface Input extends Closeable {
      Object readObject();

      boolean eof();

      @Override
      void close();
    }
  }
}
