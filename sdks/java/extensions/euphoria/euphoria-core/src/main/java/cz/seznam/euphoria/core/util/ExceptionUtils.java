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

import java.util.function.Consumer;

/**
 * Utils for easier exception handling.
 */
public class ExceptionUtils {

  /**
   * Catches any exception thrown by provided {@link Supplier}
   * and rethrows it as {@link RuntimeException}.
   *
   * @param supplier to provide value, that can throw checked exception
   * @param <T> type of value the supplier returns
   * @return supplied value
   */
  public static <T> T unchecked(Supplier<T> supplier) {
    try {
      return supplier.apply();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      // no need to wrap, it is already unchecked
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Catches any exception thrown by provided {@link Action}
   * and rethrows it as {@link RuntimeException}.
   *
   * @param action that can throw checked exception
   */
  public static void unchecked(Action action) {
    try {
      action.apply();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      // no need to wrap, it is already unchecked
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return consumer throwing {@link RuntimeException} on each exception
   * thrown from given {@link ThrowingConsumer}.
   * @param <T> type parameter
   * @param consumer the consumer that throws exception
   * @return consumer not throwing checked exceptions
   */
  public static <T> Consumer<T> unchecked(ThrowingConsumer<T> consumer) {
    return what -> {
      try {
        consumer.consume(what);
      } catch (RuntimeException ex) {
        // no need to wrap, it is already unchecked
        throw ex;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  /**
   * Create supplier for {@link IllegalStateException} with given message.
   * @param message the message for exception
   * @return exception supplier
   */
  public static java.util.function.Supplier<IllegalStateException> illegal(String message) {
    return () -> new IllegalStateException(message);
  }

  @FunctionalInterface
  public interface Supplier<T> {
    T apply() throws Exception;
  }

  @FunctionalInterface
  public interface Action {
    void apply() throws Exception;
  }

  @FunctionalInterface
  public interface ThrowingConsumer<T> {
    public void consume(T what) throws Exception;
  }
}
