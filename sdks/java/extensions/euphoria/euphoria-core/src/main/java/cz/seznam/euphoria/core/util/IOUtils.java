/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
