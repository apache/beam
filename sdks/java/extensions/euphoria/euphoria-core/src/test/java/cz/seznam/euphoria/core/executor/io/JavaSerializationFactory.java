/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.NoSuchElementException;

public class JavaSerializationFactory implements SerializerFactory {

  static class JavaSerializer implements Serializer {

    static class ObjectOutputStreamAdapter implements Output {
      private final ObjectOutputStream out;

      ObjectOutputStreamAdapter(ObjectOutputStream out) {
        this.out = out;
      }

      @Override
      public void writeObject(Object o) {
        try {
          out.writeObject(o);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void flush() {
        try {
          out.flush();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void close() {
        try {
          out.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    static class ObjectInputStreamAdapter implements Input {
      private final ObjectInputStream in;

      private boolean finished;
      private Object next;

      public ObjectInputStreamAdapter(ObjectInputStream in) {
        this.in = in;
      }

      @Override
      public Object readObject() {
        if (next == null) {
          advance();
        }
        if (finished && next == null) {
          throw new NoSuchElementException("end of file");
        }
        Object n = next;
        next = null;
        return n;
      }

      @Override
      public boolean eof() {
        if (!finished && next == null) {
          advance();
        }
        return finished && next == null;
      }

      private void advance() {
        if (finished) {
          return;
        }
        try {
          next = in.readObject();
        } catch (IOException | ClassNotFoundException e) {
          finished = true;
          next = null;
        }
      }

      @Override
      public void close() {
        try {
          in.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public Output newOutput(java.io.OutputStream out) {
      try {
        return new ObjectOutputStreamAdapter(new ObjectOutputStream(out));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Input newInput(java.io.InputStream in) {
      try {
        return new ObjectInputStreamAdapter(new ObjectInputStream(in));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Serializer newSerializer() {
    return new JavaSerializer();
  }
}