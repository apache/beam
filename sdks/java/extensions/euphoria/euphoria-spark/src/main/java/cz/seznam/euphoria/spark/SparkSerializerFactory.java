package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.executor.storage.SerializerFactory;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.io.EOFException;
import java.util.NoSuchElementException;
import java.util.Objects;

class SparkSerializerFactory implements SerializerFactory {

  // ~ adapts spark's SerializerInstance to
  // euphoria's SerializerFactory.Serializer
  static class SparkSerializer implements Serializer {

    // ~ adapts spark's SerializationStream to
    // euphoria's SerializerFactory.Serializer.OutputStream
    static class SparkSerializerOutputStream implements OutputStream {
      private final SerializationStream sparkSerializationStream;
      private ClassTag type;

      SparkSerializerOutputStream(SerializationStream s) {
        sparkSerializationStream = Objects.requireNonNull(s);
      }

      @SuppressWarnings("unchecked")
      @Override
      public void writeObject(Object o) {
        if (type == null) {
          type = ClassTag$.MODULE$.apply(o.getClass());
        }
        sparkSerializationStream.writeObject(o, type);
      }

      @Override
      public void flush() {
        sparkSerializationStream.flush();
      }

      @Override
      public void close() {
        sparkSerializationStream.close();
      }
    }

    // ~ adopts spark's DeserializationStream to
    // euphoria's SerializerFactory.Serializer.InputStream
    static class SparkSerializerInputStream implements InputStream {
      private final DeserializationStream sparkDeserializationStream;

      private boolean streamFinished;
      private Object next;

      SparkSerializerInputStream(DeserializationStream s) {
        sparkDeserializationStream = s;
      }

      @Override
      public Object readObject() {
        while (true) {
          if (next != null) {
            Object n = next;
            next = null;
            return n;
          }
          if (streamFinished) {
            throw new NoSuchElementException("End of file reached!");
          }
          tryReadNext();
        }
      }

      @SuppressWarnings("BC_IMPOSSIBLE_INSTANCEOF")
      private void tryReadNext() {
        try {
          next = sparkDeserializationStream.readObject(ClassTag$.MODULE$.Any());
        } catch (Exception e) {
          // ~ need to go with this ugly instanceof construct since the
          // scala method does not declare the exception as part of the
          // readObject method's signature. but it actually does throw it
          // to signal the end-of-stream.
          if (EOFException.class.isAssignableFrom(e.getClass())) {
            System.out.println("hoho!!!");
            streamFinished = true;
          } else {
            throw e;
          }
        }
      }

      @Override
      public boolean eof() {
        if (next != null) {
          return false;
        }
        if (streamFinished) {
          return true;
        } else {
          tryReadNext();
          return next == null && streamFinished;
        }
      }

      @Override
      public void close() {
        sparkDeserializationStream.close();
      }
    }

    private final SerializerInstance sparkSerializerInstance;

    SparkSerializer(SerializerInstance sparkSerializerInstance) {
      this.sparkSerializerInstance = Objects.requireNonNull(sparkSerializerInstance);
    }

    @Override
    public OutputStream newOutputStream(java.io.OutputStream out) {
      return new SparkSerializerOutputStream(sparkSerializerInstance.serializeStream(out));
    }

    @Override
    public InputStream newInputStream(java.io.InputStream in) {
      return new SparkSerializerInputStream(sparkSerializerInstance.deserializeStream(in));
    }
  }

  private final org.apache.spark.serializer.Serializer sparkSerializer;

  SparkSerializerFactory(org.apache.spark.serializer.Serializer sparkSerializer) {
    this.sparkSerializer = Objects.requireNonNull(sparkSerializer);
  }

  @Override
  public Serializer newSerializer() {
    return new SparkSerializer(sparkSerializer.newInstance());
  }
}
