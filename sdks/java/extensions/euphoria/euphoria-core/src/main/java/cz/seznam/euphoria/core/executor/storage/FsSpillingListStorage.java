package cz.seznam.euphoria.core.executor.storage;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class FsSpillingListStorage<T> implements ListStorage<T> {

  private final SerializerFactory serializerFactory;
  private final int maxElemsInMemory;

  // ~ new elements are appended to this list and eventually this list
  // will be spilled to disk. therefore, this list contains the lastly
  // added elements to this list-storage
  private final List<T> elems = new ArrayList<>();

  private File storageFile;
  private SerializerFactory.Serializer serializerInstance;
  private SerializerFactory.Serializer.OutputStream serializerStream;
  private boolean needsFlush;

  public FsSpillingListStorage(SerializerFactory serializerFactory, int maxElemsInMemory) {
    this.serializerFactory = Objects.requireNonNull(serializerFactory);
    this.maxElemsInMemory = maxElemsInMemory;
  }

  @Override
  public void add(T element) {
    elems.add(element);
    if (elems.size() >= maxElemsInMemory) {
      spillElems();
    }
  }

  private void spillElems() {
    if (serializerStream == null) {
      initDiskStorage();
    }
    for (T elem : elems) {
      serializerStream.writeObject(elem);
    }
    elems.clear();
    needsFlush = true;
  }

  @Override
  public Iterable<T> get() {
    return () -> {
      if (serializerStream == null && elems.isEmpty()) {
        return Collections.emptyIterator();
      }

      SerializerFactory.Serializer.InputStream is;
      if (serializerStream != null) {
        if (needsFlush) {
          serializerStream.flush();
          needsFlush = false;
        }
        try {
          is = serializerInstance.newInputStream(new FileInputStream(storageFile));
        } catch (FileNotFoundException e) {
          throw new IllegalStateException("Failed to open spilling storage: " + storageFile, e);
        }
      } else {
        is = null;
      }

      Iterator elemsIter = elems.iterator();
      return new Iterator<T>() {
        @Override
        public boolean hasNext() {
          boolean n = (is != null && !is.eof()) || elemsIter.hasNext();
          if (!n && is != null) {
            is.close();
          }
          return n;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T next() {
          if (is != null && !is.eof()) {
            return (T) is.readObject();
          }
          if (elemsIter.hasNext()) {
            return (T) elemsIter.next();
          }
          if (is != null) {
            is.close();
          }
          throw new NoSuchElementException();
        }
      };
    };
  }

  @Override
  public void clear() {
    if (serializerStream != null) {
      serializerStream.close();
      if (!storageFile.delete()) {
        throw new IllegalStateException("Failed to clean up storage file: " + storageFile);
      }
    }
  }

  private void initDiskStorage() {
    assert storageFile == null;
    assert serializerInstance == null;
    assert serializerStream == null;
    try {
      Path workDir = FileSystems.getFileSystem(new URI("file:///")).getPath("./");
      storageFile = Files.createTempFile(workDir, getClass().getName(), ".dat").toFile();
      serializerInstance = serializerFactory.newSerializer();
      serializerStream = serializerInstance.newOutputStream(new FileOutputStream(storageFile));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}