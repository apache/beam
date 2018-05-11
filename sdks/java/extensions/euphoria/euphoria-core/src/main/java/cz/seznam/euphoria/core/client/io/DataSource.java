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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import java.io.Serializable;

/** Source of data for dataset. */
@Audience(Audience.Type.CLIENT)
public interface DataSource<T> extends Serializable {

  /**
   * @return {@code true} if this source is bounded, {@code false} if it is unbounded or it is not
   *     known if it is bounded or unbounded.
   */
  boolean isBounded();

  /** @return {@code true} if this is not bounded source, {@code false} otherwise */
  default boolean isUnbounded() {
    return !isBounded();
  }

  /**
   * Retrieve batch {@code DataSource}.
   *
   * @return {@code BoundedDataSource} if this is bounded source
   * @throws UnsupportedOperationException if this is not {@code BoundedDataSource}.
   */
  default BoundedDataSource<T> asBounded() {
    throw new UnsupportedOperationException("Not supported.");
  }

  /**
   * Retrieve stream {@code DataSource}.
   *
   * @return {@code UnboundedDataSource} if this is unbounded source
   * @throws UnsupportedOperationException if this is not {@code UnboundedDataSource}.
   */
  default UnboundedDataSource<T, ?> asUnbounded() {
    throw new UnsupportedOperationException("Not supported.");
  }
}
