package cz.seznam.euphoria.core.client.io;

import java.io.IOException;
import java.util.NoSuchElementException;

class EmptyReader<T> implements Reader<T> {
  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public T next() {
    throw new NoSuchElementException();
  }
}
