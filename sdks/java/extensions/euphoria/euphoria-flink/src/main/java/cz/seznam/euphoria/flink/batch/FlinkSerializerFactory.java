/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
            ExecutionConfig.SerializableSerializer<?> flinkSerializer = flinkSerializers.get(clz);
            if (flinkSerializer == null) {
              serializer = null;
            } else {
              serializer = flinkSerializer.getSerializer();
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
