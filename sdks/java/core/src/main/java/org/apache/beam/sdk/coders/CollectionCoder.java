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
package org.apache.beam.sdk.coders;

import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

/**
 * A {@link CollectionCoder} encodes {@link Collection Collections} in the format of {@link
 * IterableLikeCoder}.
 */
public class CollectionCoder<T> extends IterableLikeCoder<T, Collection<T>> {

  public static <T> CollectionCoder<T> of(Coder<T> elemCoder) {
    return new CollectionCoder<>(elemCoder);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.

  /**
   * {@inheritDoc}
   *
   * @return the decoded elements directly, since {@link List} is a subtype of {@link Collection}.
   */
  @Override
  protected final Collection<T> decodeToIterable(List<T> decodedElements) {
    return decodedElements;
  }

  protected CollectionCoder(Coder<T> elemCoder) {
    super(elemCoder, "Collection");
  }

  @Override
  public TypeDescriptor<Collection<T>> getEncodedTypeDescriptor() {
    return new TypeDescriptor<Collection<T>>() {}.where(
        new TypeParameter<T>() {}, getElemCoder().getEncodedTypeDescriptor());
  }
}
