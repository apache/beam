/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.portability;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An {@link AutoCloseable} that wraps a resource that needs to be cleaned up but does not implement
 * {@link AutoCloseable} itself.
 *
 * <p>Recipients of a {@link CloseableResource} are in general responsible for cleanup. Ownership
 * can be transferred from one context to another via {@link #transfer()}. Transferring relinquishes
 * ownership from the original resource. This allows resources to be safely constructed and
 * transferred within a try-with-resources block. For example:
 *
 * <p>{@code try (CloseableResource<Foo> resource = CloseableResource.of(...)) { // Do something
 * with resource. ... // Then transfer ownership to some consumer.
 * resourceConsumer(resource.transfer()); } }
 *
 * <p>Not thread-safe.
 */
public class CloseableResource<T> implements AutoCloseable {

  private final T resource;

  /**
   * {@link Closer } for the underlying resource. Closers are nullable to allow transfer of
   * ownership. However, newly-constructed {@link CloseableResource CloseableResources} must always
   * have non-null closers.
   */
  private @Nullable Closer<T> closer;

  private boolean isClosed = false;

  private CloseableResource(T resource, Closer<T> closer) {
    this.resource = resource;
    this.closer = closer;
  }

  /** Creates a {@link CloseableResource} with the given resource and closer. */
  public static <T> CloseableResource<T> of(T resource, Closer<T> closer) {
    checkArgument(resource != null, "Resource must be non-null");
    checkArgument(closer != null, "%s must be non-null", Closer.class.getName());
    return new CloseableResource<>(resource, closer);
  }

  /** Gets the underlying resource. */
  public T get() {
    checkState(closer != null, "%s has transferred ownership", CloseableResource.class.getName());
    checkState(!isClosed, "% is closed", CloseableResource.class.getName());
    return resource;
  }

  /**
   * Returns a new {@link CloseableResource} that owns the underlying resource and relinquishes
   * ownership from this {@link CloseableResource}. {@link #close()} on the original instance
   * becomes a no-op.
   */
  public CloseableResource<T> transfer() {
    checkState(closer != null, "%s has transferred ownership", CloseableResource.class.getName());
    checkState(!isClosed, "% is closed", CloseableResource.class.getName());
    CloseableResource<T> other = CloseableResource.of(resource, closer);
    this.closer = null;
    return other;
  }

  /**
   * Closes the underlying resource. The closer will only be executed on the first call.
   *
   * @throws CloseException wrapping any exceptions thrown while closing
   */
  @Override
  public void close() throws CloseException {
    if (closer != null && !isClosed) {
      try {
        closer.close(resource);
      } catch (Exception e) {
        throw new CloseException(e);
      } finally {
        // Mark resource as closed even if we catch an exception.
        isClosed = true;
      }
    }
  }

  /** A function that knows how to clean up after a resource. */
  @FunctionalInterface
  public interface Closer<T> {
    void close(T resource) throws Exception;
  }

  /** An exception that wraps errors thrown while a resource is being closed. */
  public static class CloseException extends Exception {
    private CloseException(Exception e) {
      super("Error closing resource", e);
    }
  }
}
