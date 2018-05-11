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

/**
 * A reader of data from unbounded partition. The reader differs from {@code BoundedReader} by the
 * fact, that it has to: * be able to reset itself to a defined position * be able to define a
 * position for each emitted element * emit watermarks along with the data elements
 *
 * @param <T> the data type contained in the source
 * @param <OFFSET> data type of offset emitted with the elements
 */
@Audience(Audience.Type.CLIENT)
public interface UnboundedReader<T, OFFSET> extends CloseableIterator<T> {

  /**
   * Retrieve offset where the reader has reached so far.
   *
   * @return the so far read offset
   */
  OFFSET getCurrentOffset();

  /**
   * Reset the reader to given offset. Call to `next` will then return element with offset following
   * the offset being reset to.
   *
   * @param offset the offset to reset to, element with offset following this one will be returned
   *     next
   */
  void reset(OFFSET offset);

  /**
   * Commit given offset as being processed.
   *
   * <p>After all results incorporating the data element emitted with given offset is processed,
   * executor might call this to commit the resulting offset.
   *
   * @param offset the offset to be committed for persistence across runs of the streaming
   *     processing
   */
  void commitOffset(OFFSET offset);
}
