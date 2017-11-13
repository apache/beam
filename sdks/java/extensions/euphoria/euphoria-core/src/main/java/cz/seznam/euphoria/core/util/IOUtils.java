package cz.seznam.euphoria.core.util;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Util class that helps iterate over methods throwing {@link IOException}.
 */
public class IOUtils {

  @FunctionalInterface
  public interface IOConsumer<T> {
    void accept(T t) throws IOException;
  }

  /**
   * Accepts consumer for each element. First occurred IOException is thrown after all elements are
   * iterated. Other IOExceptions are appended as suppressed.
   * @param iterable list of elements
   * @param consumer that performs accept operation
   * @param <T> type of element
   * @throws IOException first occurred IOException
   */
  public static <T> void forEach(Iterable<T> iterable, IOConsumer<T> consumer) throws IOException {
    IOException firstException = null;
    for (T element : iterable) {
      try {
        consumer.accept(element);
      } catch (IOException e) {
        if (firstException != null) {
          firstException.addSuppressed(e);
        } else {
          firstException = e;
        }
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  public static <T> void forEach(Stream<T> stream, IOConsumer<T> consumer) throws IOException {
    forEach(stream::iterator, consumer);
  }

}
