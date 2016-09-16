
package cz.seznam.euphoria.flink.batch;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import cz.seznam.euphoria.core.client.operator.state.ListStateStorage;
import cz.seznam.euphoria.core.client.operator.state.StateStorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStateStorage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
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
 * storage is used in batch only and can therefore by reconstructed by
 * recalculation of the data.
 */
class BatchStateStorageProvider implements StateStorageProvider, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BatchStateStorageProvider.class);

  @SuppressWarnings("unchecked")
  static class ValueStorage implements ValueStateStorage {

      Object value;

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
  static class ListStorage implements ListStateStorage {

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

    

    ListStorage(
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
        input = serializedElements.exists()
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
      try {
        data.clear();
        if (serializedElements != null) {
          serializedElements.delete();
        }
        this.serializedElements = File.createTempFile("euphoria-state-storage", ".bin");
        serializedElements.deleteOnExit();
        this.output = new Output(new FileOutputStream(serializedElements));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    private void flush() throws IOException {

      if (data.isEmpty())
        return;

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
  public <T> ValueStateStorage<T> getValueStorageFor(Class<T> what) {
    initKryo();
    // this is purely in memory
    return new ValueStorage();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ListStateStorage<T> getListStorageFor(Class<T> what) {
    try {
      initKryo();
      return new ListStorage(MAX_ELEMENTS_IN_MEMORY, kryo, serializers);
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
