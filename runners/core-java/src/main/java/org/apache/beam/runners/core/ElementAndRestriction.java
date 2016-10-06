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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A tuple of an element and a restriction applied to processing it with a
 * <a href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
 */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
@AutoValue
public abstract class ElementAndRestriction<ElementT, RestrictionT> {
  /** The element to process. */
  public abstract ElementT element();

  /** The restriction applied to processing the element. */
  public abstract RestrictionT restriction();

  /** Constructs the {@link ElementAndRestriction}. */
  public static <InputT, RestrictionT> ElementAndRestriction<InputT, RestrictionT> of(
      InputT element, RestrictionT restriction) {
    return new AutoValue_ElementAndRestriction<>(element, restriction);
  }
}
