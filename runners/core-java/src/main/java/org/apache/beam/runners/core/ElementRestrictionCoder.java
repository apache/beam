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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;

/** A {@link Coder} for {@link ElementRestriction}. */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class ElementRestrictionCoder<ElementT, RestrictionT>
    extends CustomCoder<ElementRestriction<ElementT, RestrictionT>> {
  private final Coder<ElementT> elementCoder;
  private final Coder<RestrictionT> restrictionCoder;

  /** Creates an {@link ElementRestrictionCoder} from an element coder and a restriction coder. */
  public static <ElementT, RestrictionT> ElementRestrictionCoder<ElementT, RestrictionT> of(
      Coder<ElementT> elementCoder, Coder<RestrictionT> restrictionCoder) {
    return new ElementRestrictionCoder<>(elementCoder, restrictionCoder);
  }

  private ElementRestrictionCoder(
      Coder<ElementT> elementCoder, Coder<RestrictionT> restrictionCoder) {
    this.elementCoder = elementCoder;
    this.restrictionCoder = restrictionCoder;
  }

  @Override
  public void encode(
      ElementRestriction<ElementT, RestrictionT> value, OutputStream outStream, Context context)
      throws IOException {
    Context nestedContext = context.nested();
    elementCoder.encode(value.element(), outStream, nestedContext);
    restrictionCoder.encode(value.restriction(), outStream, nestedContext);
  }

  @Override
  public ElementRestriction<ElementT, RestrictionT> decode(InputStream inStream, Context context)
      throws IOException {
    Context nestedContext = context.nested();
    ElementT key = elementCoder.decode(inStream, nestedContext);
    RestrictionT value = restrictionCoder.decode(inStream, nestedContext);
    return ElementRestriction.of(key, value);
  }
}
