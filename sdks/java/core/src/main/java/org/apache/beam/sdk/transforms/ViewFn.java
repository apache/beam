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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>A function to adapt a primitive "view" of a {@link PCollection} - some materialization
 * specified in the Beam model and implemented by the runner - to a user-facing view type for side
 * input.
 *
 * <p>Both the underlying primitive view and the user-facing view are immutable.
 *
 * <p>The most common case is using the {@link View} transforms to prepare a {@link PCollection} for
 * use as a side input to {@link ParDo}. See {@link View#asSingleton()}, {@link View#asIterable()},
 * and {@link View#asMap()} for more detail on specific views available in the SDK.
 *
 * @param <PrimitiveViewT> the type of the underlying primitive view required
 * @param <ViewT> the type of the value(s) accessible via this {@link PCollectionView}
 */
@Internal
public abstract class ViewFn<PrimitiveViewT, ViewT> implements Serializable {
  /** Gets the materialization of this {@link ViewFn}. */
  public abstract Materialization<PrimitiveViewT> getMaterialization();

  /** A function to adapt a primitive view type to a desired view type. */
  public abstract ViewT apply(PrimitiveViewT primitiveViewT);

  /** Return the {@link TypeDescriptor} describing the output of this fn. */
  public abstract TypeDescriptor<ViewT> getTypeDescriptor();
}
