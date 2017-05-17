package cz.seznam.euphoria.core.executor.storage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.NoSuchElementException;

public class JavaSerializationFactory implements SerializerFactory {

  static class JavaSerializer implements Serializer {

    static class ObjectOutputStreamAdapter implements OutputStream {
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

    static class ObjectInputStreamAdapter implements InputStream {
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
    public OutputStream newOutputStream(java.io.OutputStream out) {
      try {
        return new ObjectOutputStreamAdapter(new ObjectOutputStream(out));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public InputStream newInputStream(java.io.InputStream in) {
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