package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that safely ensures an object of type T is cleaned up before it is garbage collected.
 */
class Refcounted<T extends AutoCloseable> implements Supplier<T> {

  private static final Logger logger = LoggerFactory.getLogger(Refcounted.class);

  private final T object;

  public static <T extends AutoCloseable> Refcounted<T> of(T object) {
    return new Refcounted<>(object);
  }

  @Override
  public T get() {
    return object;
  }

  private Refcounted(T object) {
    this.object = object;
  }

  @Override
  protected void finalize() {
    SystemExecutors.getFuturesExecutor().execute(() -> {
      try {
        object.close();
      } catch (Exception e) {
        logger.warn("Failed to close resource with class: " + object.getClass().getCanonicalName(),
            e);
      }
    });
  }
}
