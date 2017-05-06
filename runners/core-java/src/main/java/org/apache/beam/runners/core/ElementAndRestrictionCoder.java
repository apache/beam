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
package org.apache.beam.runners.core;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;

/** A {@link Coder} for {@link ElementAndRestriction}. */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class ElementAndRestrictionCoder<ElementT, RestrictionT>
    extends StructuredCoder<ElementAndRestriction<ElementT, RestrictionT>> {
  private final Coder<ElementT> elementCoder;
  private final Coder<RestrictionT> restrictionCoder;

  /**
   * Creates an {@link ElementAndRestrictionCoder} from an element coder and a restriction coder.
   */
  public static <ElementT, RestrictionT> ElementAndRestrictionCoder<ElementT, RestrictionT> of(
      Coder<ElementT> elementCoder, Coder<RestrictionT> restrictionCoder) {
    return new ElementAndRestrictionCoder<>(elementCoder, restrictionCoder);
  }

  private ElementAndRestrictionCoder(
      Coder<ElementT> elementCoder, Coder<RestrictionT> restrictionCoder) {
    this.elementCoder = elementCoder;
    this.restrictionCoder = restrictionCoder;
  }

  @Override
  public void encode(
      ElementAndRestriction<ElementT, RestrictionT> value, OutputStream outStream)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null ElementAndRestriction");
    }
    elementCoder.encode(value.element(), outStream);
    restrictionCoder.encode(value.restriction(), outStream);
  }

  @Override
  public ElementAndRestriction<ElementT, RestrictionT> decode(InputStream inStream)
      throws IOException {
    ElementT key = elementCoder.decode(inStream);
    RestrictionT value = restrictionCoder.decode(inStream);
    return ElementAndRestriction.of(key, value);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(elementCoder, restrictionCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    elementCoder.verifyDeterministic();
    restrictionCoder.verifyDeterministic();
  }

  public Coder<ElementT> getElementCoder() {
    return elementCoder;
  }

  public Coder<RestrictionT> getRestrictionCoder() {
    return restrictionCoder;
  }
}
