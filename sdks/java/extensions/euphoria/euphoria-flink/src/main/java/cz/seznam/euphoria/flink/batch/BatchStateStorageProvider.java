
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
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  @SuppressWarnings("unchecked")
  static class MemValueStorage implements ValueStorage {

      Object value;

      MemValueStorage(Object defVal) {
        this.value = defVal;
      }

      @Override
      public void set(Object value) {
        this.value = value;
      }

      @Override
      public Object get() {
        return value;
      }

      @Override
      public void clear() {
        value = null;
      }
  }

  @SuppressWarnings("unchecked")
  static class MemListStorage implements ListStorage {

    final int MAX_ELEMENTS_IN_MEMORY;
    final Kryo kryo;
    final LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>> serializers;

    // the class that we will serialize
    Class clz;
    // serializer for the class
    Serializer serializer;
    List data = new ArrayList();
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

    @Override
    public void add(Object element) {
      if (clz == null) {
        clz = element.getClass();
        ExecutionConfig.SerializableSerializer<?> serializedSerializer = serializers.get(clz);
        if (serializedSerializer == null) {
          LOG.warn("No registered serializer for class {} using kryo default", clz);
          this.serializer = null;
        } else {
          this.serializer = serializedSerializer.getSerializer();
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
    public Iterable get() {

      Input input;
      try {
        input = serializedElements != null
                ? new Input(IOUtils.toBufferedInputStream(new FileInputStream(serializedElements)))
                : null;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      Iterator dataIterator = data.iterator();
      return () -> {
          return new Iterator() {
            @Override
            public boolean hasNext() {
              return  input != null && !input.eof()
                  || dataIterator.hasNext();
            }

            @Override
            public Object next() {
              if (input != null && !input.eof()) {
                final Object read;
                if (serializer != null) {
                  read = serializer.read(kryo, input, clz);
                } else {
                  read = kryo.readObject(input, clz);
                }
                return read;
              }
              if (dataIterator.hasNext())
                return dataIterator.next();
              throw new NoSuchElementException();
            }

          };
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
        for (Object o : data) {
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
  @SuppressWarnings("uncheced")
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    initKryo();
    // this is purely in memory
    return new MemValueStorage(descriptor.getDefaultValue());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    try {
      initKryo();
      return new MemListStorage(MAX_ELEMENTS_IN_MEMORY, kryo, serializers);
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
