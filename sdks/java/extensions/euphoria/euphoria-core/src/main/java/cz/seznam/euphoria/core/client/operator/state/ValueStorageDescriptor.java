/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.operator.state;

import cz.seznam.euphoria.core.client.functional.BinaryFunction;

/**
 * Descriptor of {@code ValueStorage}.
 *
 * @param <T> the type of value referred to through this descriptor
 */
public class ValueStorageDescriptor<T> extends StorageDescriptor {

  public static final class MergingValueStorageDescriptor<T>
      extends ValueStorageDescriptor<T>
      implements MergingStorageDescriptor<T> {

    private final BinaryFunction<T, T, T> merger;

    MergingValueStorageDescriptor(
        String name, Class<T> cls, T defVal,
        BinaryFunction<T, T, T> merger) {
      super(name, cls, defVal);
      this.merger = merger;
    }

    @Override
    public BinaryFunction<ValueStorage<T>, ValueStorage<T>, Void> getMerger() {
      return (l, r) -> {
        l.set(getValueMerger().apply(l.get(), r.get()));
        return null;
      };
    }

    public BinaryFunction<T, T, T> getValueMerger() {
      return merger;
    }
  }

  /**
   * Get descriptor of value storage without merging.
   *
   * @param <T> the type of value referred to through the new descriptor
   *
   * @param name a name of the storage
   * @param cls the type of the value stored in the storage
   * @param defVal the default value to be provided in case no such is yet stored
   *
   * @return a new descriptor for a value storage
   */
  public static <T> ValueStorageDescriptor<T> of(String name, Class<T> cls, T defVal) {
    return new ValueStorageDescriptor<>(name, cls, defVal);
  }

  /**
   * Get mergeable value storage descriptor.
   * This is needed in conjunction with all merging windowings
   * and for all state storages.
   *
   * @param <T> the type of value referred to through the new descriptor
   *
   * @param name a name of the storage
   * @param cls the type of the value stored in the storage
   * @param defVal the default value to be provided in case no such is yet stored
   * @param merger the merge function to utilize upon state value updates
   *
   * @return a new descriptor for a value storage
   */
  public static <T> ValueStorageDescriptor<T> of(
      String name, Class<T> cls, T defVal, BinaryFunction<T, T, T> merger) {
    return new MergingValueStorageDescriptor<>(name, cls, defVal, merger);
  }

  private final Class<T> cls;
  private final T defVal;

  private ValueStorageDescriptor(String name, Class<T> cls, T defVal) {
    super(name);
    this.cls = cls;
    this.defVal = defVal;
  }  
  
  public Class<T> getValueClass() { return cls; }
  
  public T getDefaultValue() { return defVal; }


}
