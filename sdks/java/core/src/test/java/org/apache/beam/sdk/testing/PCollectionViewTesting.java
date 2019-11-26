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
package org.apache.beam.sdk.testing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/** Methods for testing {@link PCollectionView}s. */
public final class PCollectionViewTesting {
  public static List<Object> materializeValuesFor(
      PTransform<?, ? extends PCollectionView<?>> viewTransformClass, Object... values) {
    List<Object> rval = new ArrayList<>();
    // Currently all view materializations are the same where the data is shared underneath
    // the void/null key. Once this changes, these materializations will differ but test code
    // should not worry about what these look like if they are relying on the ViewFn to "undo"
    // the conversion.
    if (View.AsSingleton.class.equals(viewTransformClass.getClass())) {
      for (Object value : values) {
        rval.add(KV.of(null, value));
      }
    } else if (View.AsIterable.class.equals(viewTransformClass.getClass())) {
      for (Object value : values) {
        rval.add(KV.of(null, value));
      }
    } else if (View.AsList.class.equals(viewTransformClass.getClass())) {
      for (Object value : values) {
        rval.add(KV.of(null, value));
      }
    } else if (View.AsMap.class.equals(viewTransformClass.getClass())) {
      for (Object value : values) {
        rval.add(KV.of(null, value));
      }
    } else if (View.AsMultimap.class.equals(viewTransformClass.getClass())) {
      for (Object value : values) {
        rval.add(KV.of(null, value));
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unknown type of view %s. Supported views are %s.",
              viewTransformClass.getClass(),
              ImmutableSet.of(
                  View.AsSingleton.class,
                  View.AsIterable.class,
                  View.AsList.class,
                  View.AsMap.class,
                  View.AsMultimap.class)));
    }
    return Collections.unmodifiableList(rval);
  }
}
