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
package org.apache.beam.sdk.util.common;

import java.util.Iterator;
import java.util.Observable;
import org.apache.beam.sdk.annotations.Internal;

/**
 * An abstract class used for iterators that notify observers about size in bytes of their elements,
 * as they are being iterated over. The subclasses need to implement the standard Iterator interface
 * and call method notifyValueReturned() for each element read and/or iterated over.
 *
 * @param <V> value type
 */
@Internal
public abstract class ElementByteSizeObservableIterator<V> extends Observable
    implements Iterator<V> {
  protected final void notifyValueReturned(long byteSize) {
    setChanged();
    notifyObservers(byteSize);
  }
}
