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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.shaded.guava.com.google.common.io.Closeables;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Storage provider for batch processing.
 * We will store the data in memory, for some amount of data and then
 * flush in on disk.
 * We don't need to worry about checkpointing here, because the
 * storage is used in batch only and can therefore be reconstructed by
 * recalculation of the data.
 */
class BatchStateStorageProvider implements StorageProvider, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BatchStateStorageProvider.class);

  static class MemValueStorage<T> implements ValueStorage<T> {

      T value;

      MemValueStorage(T defVal) {
        this.value = defVal;
      }

      @Override
      public void set(T value) {
        this.value = value;
      }

      @Override
      public T get() {
        return value;
      }

      @Override
      public void clear() {
        value = null;
      }
  }

  static class MemListStorage<T> implements ListStorage<T> {

    final int MAX_ELEMENTS_IN_MEMORY;
    final Kryo kryo;
    final LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> serializers;

    // the class that we will serialize
    Class<T> clz;
    // serializer for the class
    Serializer<T> serializer;
    List<T> data = new ArrayList<>();
    @Nullable
    File serializedElements = null;
    Output output = null;

    MemListStorage(
        int maxElementsInMemory,
        Kryo kryo,
        final LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> serializers)
        throws IOException {

      this.MAX_ELEMENTS_IN_MEMORY = maxElementsInMemory;
      this.kryo = kryo;
      this.serializers = serializers;
      clear();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(T element) {
      if (clz == null) {
        clz = (Class<T>) element.getClass();
        ExecutionConfig.SerializableSerializer<?> serializedSerializer = serializers.get(clz);
        if (serializedSerializer == null) {
          LOG.warn("No registered serializer for class {} using kryo default", clz);
          this.serializer = null;
        } else {
          this.serializer = (Serializer<T>) serializedSerializer.getSerializer();
        }
      } else if (element.getClass() != clz) {
        throw new IllegalArgumentException(
            "Use only single class as a storage type, got " + clz
                + " and " + element.getClass());
      }
      data.add(element);
      if (data.size() > MAX_ELEMENTS_IN_MEMORY) {
        try {
          flush();
          data.clear();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    @Override
    public Iterable<T> get() {
      return this::iter;
    }

    private Iterator<T> iter() {
      FileInputStream finput;
      Input input;
      try {
        if (serializedElements == null) {
          finput = null;
          input = null;
        } else {
          finput = new FileInputStream(serializedElements);
          input = new Input(IOUtils.toBufferedInputStream(finput));
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      Iterator<T> dataIterator = data.iterator();
      return new Iterator<T>() {
        @Override
        public boolean hasNext() {
          boolean n = input != null && !input.eof() || dataIterator.hasNext();
          if (!n && input != null) {
            input.close();
            Closeables.closeQuietly(finput);
          }
          return n;
        }

        @Override
        public T next() {
          if (input != null && !input.eof()) {
            final T read;
            if (serializer != null) {
              read = serializer.read(kryo, input, clz);
            } else {
              read = kryo.readObject(input, clz);
            }
            return read;
          }
          if (dataIterator.hasNext()) {
            return dataIterator.next();
          }
          if (input != null) {
            input.close();
            Closeables.closeQuietly(finput);
          }
          throw new NoSuchElementException();
        }
      };
    }

    @Override
    public final void clear() {
      data.clear();
      if (serializedElements != null) {
        serializedElements.delete();
      }
      serializedElements = null;
    }

    private void flush() throws IOException {

      if (data.isEmpty()) {
        return;
      }

      if (serializedElements == null) {
        initDiskStorage();
      }

      try {
        for (T o : data) {
          if (serializer != null) {
            serializer.write(kryo, output, o);
          } else {
            kryo.writeObject(output, o);
          }
        }
      } finally {
        output.flush();
      }
           
    }

    private void initDiskStorage() throws IOException {
      try {
        Path workDir = FileSystems.getFileSystem(new URI("file:///")).getPath("./");

        this.serializedElements = Files.createTempFile(workDir,
            "euphoria-state-provider", ".bin").toFile();
        this.output = new Output(new FileOutputStream(serializedElements));
      } catch (URISyntaxException ex) {
        // should not happen
        throw new IllegalStateException("This should not happen, fix code!", ex);
      }
    }

  }

  final int MAX_ELEMENTS_IN_MEMORY;  
  final LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> serializers;
  transient Kryo kryo;

  BatchStateStorageProvider(int maxInMemElements, ExecutionEnvironment env) {
    this.MAX_ELEMENTS_IN_MEMORY = maxInMemElements;
    // FIXME: how to get to the kryo instance in flink?
    this.kryo = null;
    serializers = env.getConfig().getDefaultKryoSerializers();
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    initKryo();
    // this is purely in memory
    return new MemValueStorage<>(descriptor.getDefaultValue());
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    try {
      initKryo();
      return new MemListStorage<>(MAX_ELEMENTS_IN_MEMORY, kryo, serializers);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void initKryo() {
    if (this.kryo == null) {
      this.kryo = new Kryo();
    }
  }


}
