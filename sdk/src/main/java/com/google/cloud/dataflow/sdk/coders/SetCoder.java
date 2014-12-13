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

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A SetCoder encodes Sets.
 *
 * @param <T> the type of the elements of the set
 */
public class SetCoder<T> extends StandardCoder<Set<T>> {

  /**
   * Produces a SetCoder with the given elementCoder.
   */
  public static <T> SetCoder<T> of(Coder<T> elementCoder) {
    return new SetCoder<>(elementCoder);
  }

  @JsonCreator
  public static SetCoder<?> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
      List<Object> components) {
    Preconditions.checkArgument(components.size() == 1,
        "Expecting 1 component, got " + components.size());
    return of((Coder<?>) components.get(0));
  }

  public Coder<T> getElementCoder() { return elementCoder; }

  /////////////////////////////////////////////////////////////////////////////

  Coder<T> elementCoder;

  SetCoder(Coder<T> elementCoder) {
    this.elementCoder = elementCoder;
  }

  @Override
  public void encode(
      Set<T> set,
      OutputStream outStream,
      Context context)
      throws IOException, CoderException  {
    DataOutputStream dataOutStream = new DataOutputStream(outStream);
    dataOutStream.writeInt(set.size());
    for (T element : set) {
      elementCoder.encode(element, outStream, context.nested());
    }
    dataOutStream.flush();
  }

  @Override
  public Set<T> decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    DataInputStream dataInStream = new DataInputStream(inStream);
    int size = dataInStream.readInt();
    Set<T> retval = new HashSet<T>();
    for (int i = 0; i < size; ++i) {
      T element = elementCoder.decode(inStream, context.nested());
      retval.add(element);
    }
    return retval;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.<Coder<?>>asList(elementCoder);
  }

  /**
   * Not all sets have a deterministic encoding.
   *
   * <p> For example, HashSet comparison does not depend on element order, so
   * two HashSet instances may be equal but produce different encodings.
   */
  @Override
  public boolean isDeterministic() {
    return false;
  }

  /**
   * Notifies ElementByteSizeObserver about the byte size of the encoded value using this coder.
   */
  @Override
  public void registerByteSizeObserver(
      Set<T> set, ElementByteSizeObserver observer, Context context)
      throws Exception {
    observer.update(4L);
    for (T element : set) {
      elementCoder.registerByteSizeObserver(element, observer, context.nested());
    }
  }
}
