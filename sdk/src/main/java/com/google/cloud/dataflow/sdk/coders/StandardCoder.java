/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import static com.google.cloud.dataflow.sdk.util.Structs.addList;

import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A StandardCoder is one that defines equality, hashing, and printing
 * via the class name and recursively using {@link #getComponents}.
 *
 * @param <T> the type of the values being transcoded
 */
public abstract class StandardCoder<T> implements Coder<T> {

  protected StandardCoder() {}

  /**
   * Returns the list of {@code Coder}s that are components of this
   * {@code Coder}.  Returns an empty list if this is an {@link AtomicCoder} (or
   * other {@code Coder} with no components).
   */
  public List<? extends Coder<?>> getComponents() {
    List<? extends Coder<?>> coderArguments = getCoderArguments();
    if (coderArguments == null) {
      return Collections.emptyList();
    } else {
      return coderArguments;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) {
      return false;
    }
    StandardCoder<?> that = (StandardCoder<?>) o;
    return this.getComponents().equals(that.getComponents());
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() * 31 + getComponents().hashCode();
  }

  @Override
  public String toString() {
    String s = getClass().getName();
    s = s.substring(s.lastIndexOf('.') + 1);
    List<? extends Coder<?>> componentCoders = getComponents();
    if (!componentCoders.isEmpty()) {
      s += "(";
      boolean first = true;
      for (Coder<?> componentCoder : componentCoders) {
        if (first) {
          first = false;
        } else {
          s += ", ";
        }
        s += componentCoder.toString();
      }
      s += ")";
    }
    return s;
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = CloudObject.forClass(getClass());

    List<? extends Coder<?>> components = getComponents();
    if (!components.isEmpty()) {
      List<CloudObject> cloudComponents = new ArrayList<>(components.size());
      for (Coder<?> coder : components) {
        cloudComponents.add(coder.asCloudObject());
      }
      addList(result, PropertyNames.COMPONENT_ENCODINGS, cloudComponents);
    }

    return result;
  }

  /**
   * StandardCoder requires elements to be fully encoded and copied
   * into a byte stream to determine the byte size of the element,
   * which is considered expensive.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(T value, Context context) {
    return false;
  }

  /**
   * Returns the size in bytes of the encoded value using this
   * coder. Derived classes override this method if byte size can be
   * computed with less computation or copying.
   */
  protected long getEncodedElementByteSize(T value, Context context)
      throws Exception {
    try {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      encode(value, os, context);
      return os.size();
    } catch (Exception exn) {
      throw new IllegalArgumentException(
          "Unable to encode element " + value + " with coder " + this, exn);
    }
  }

  /**
   * Notifies ElementByteSizeObserver about the byte size of the
   * encoded value using this coder.  Calls
   * getEncodedElementByteSize() and notifies ElementByteSizeObserver.
   */
  @Override
  public void registerByteSizeObserver(
      T value, ElementByteSizeObserver observer, Context context)
      throws Exception {
    observer.update(getEncodedElementByteSize(value, context));
  }
}
