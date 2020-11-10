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

import static org.apache.beam.sdk.options.ExperimentalOptions.hasExperiment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews.ValueOrMetadata;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/** Methods for testing {@link PCollectionView}s. */
public final class PCollectionViewTesting {
  public static List<Object> materializeValuesFor(
      PipelineOptions options,
      PTransform<?, ? extends PCollectionView<?>> viewTransformClass,
      Object... values) {
    List<Object> rval = new ArrayList<>();
    // Map values to the materialized format that is expected by each view type. These
    // materializations will differ but test code should not worry about what these look like if
    // they are relying on the ViewFn to "undo" the conversion.

    // TODO(BEAM-10097): Make this the default case once all portable runners can support
    // the iterable access pattern.
    if (hasExperiment(options, "beam_fn_api")
        && (hasExperiment(options, "use_runner_v2")
            || hasExperiment(options, "use_unified_worker"))) {
      if (View.AsSingleton.class.equals(viewTransformClass.getClass())) {
        for (Object value : values) {
          rval.add(value);
        }
      } else if (View.AsIterable.class.equals(viewTransformClass.getClass())) {
        for (Object value : values) {
          rval.add(value);
        }
      } else if (View.AsList.class.equals(viewTransformClass.getClass())) {
        if (values.length > 0) {
          rval.add(
              KV.of(
                  Long.MIN_VALUE,
                  ValueOrMetadata.createMetadata(new OffsetRange(0, values.length))));
          for (int i = 0; i < values.length; ++i) {
            rval.add(KV.of((long) i, ValueOrMetadata.create(values[i])));
          }
        }
      } else if (View.AsMap.class.equals(viewTransformClass.getClass())) {
        for (Object value : values) {
          rval.add(value);
        }
      } else if (View.AsMultimap.class.equals(viewTransformClass.getClass())) {
        for (Object value : values) {
          rval.add(value);
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
    } else {
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
    }
    return Collections.unmodifiableList(rval);
  }
}
