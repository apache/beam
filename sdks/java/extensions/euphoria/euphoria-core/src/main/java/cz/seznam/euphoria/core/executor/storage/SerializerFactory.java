package cz.seznam.euphoria.core.executor.storage;

import java.io.Serializable;

public interface SerializerFactory extends Serializable {

  interface Serializer {

    interface OutputStream {
      void writeObject(Object o);
      void flush();
      void close();
    }

    interface InputStream {
      Object readObject();
      boolean eof();
      void close();
    }

    OutputStream newOutputStream(java.io.OutputStream out);
    InputStream newInputStream(java.io.InputStream in);
  }

  Serializer newSerializer();
}
