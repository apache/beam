package cz.seznam.euphoria.flink.batch;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import cz.seznam.euphoria.core.executor.storage.SerializerFactory;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.LinkedHashMap;

class FlinkSerializerFactory implements SerializerFactory {

  static class FlinkSerializerAdapter implements Serializer {
    private final LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> flinkSerializers;
    private final Kryo kryo;

    // the class that we will serialize
    Class clz;
    // serializer for the class
    com.esotericsoftware.kryo.Serializer serializer;

    FlinkSerializerAdapter(
        LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> flinkSerializers,
        Kryo kryo) {
      this.flinkSerializers = flinkSerializers;
      this.kryo = kryo;
    }

    @Override
    public OutputStream newOutputStream(java.io.OutputStream os) {
      Output output = new Output(os);
      return new OutputStream() {
        @SuppressWarnings("unchecked")
        @Override
        public void writeObject(Object element) {
          if (clz == null) {
            clz = (Class) element.getClass();
            ExecutionConfig.SerializableSerializer<?> serializedSerializer = flinkSerializers.get(clz);
            if (serializedSerializer == null) {
              serializer = null;
            } else {
              serializer = serializedSerializer.getSerializer();
            }
          } else if (element.getClass() != clz) {
            throw new IllegalArgumentException(
                "Use only single class as a storage type, got " + clz
                    + " and " + element.getClass());
          }

          if (serializer == null) {
            kryo.writeObject(output, element);
          } else {
            serializer.write(kryo, output, element);
          }
        }

        @Override
        public void flush() {
          output.flush();
        }

        @Override
        public void close() {
          output.close();
        }
      };
    }

    @Override
    public InputStream newInputStream(java.io.InputStream is) {
      Input input = new Input(is);
      return new InputStream() {
        @SuppressWarnings("unchecked")
        @Override
        public Object readObject() {
          return (serializer == null)
              ? kryo.readObject(input, clz)
              : serializer.read(kryo, input, clz);
        }

        @Override
        public boolean eof() {
          return input.eof();
        }

        @Override
        public void close() {
          input.close();
        }
      };
    }
  }

  final LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> serializers;
  transient Kryo kryo;

  FlinkSerializerFactory(ExecutionEnvironment env) {
    this.serializers = env.getConfig().getDefaultKryoSerializers();
  }

  @Override
  public Serializer newSerializer() {
    return new FlinkSerializerAdapter(serializers, initKryo());
  }

  private Kryo initKryo() {
    if (this.kryo == null) {
      // FIXME: how to get to the kryo instance in flink?
      this.kryo = new Kryo();
    }
    return this.kryo;
  }
}
