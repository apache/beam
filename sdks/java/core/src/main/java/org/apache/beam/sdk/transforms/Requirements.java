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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;

/** Describes the run-time requirements of a {@link Contextful}, such as access to side inputs. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public final class Requirements implements Serializable {
  private final Collection<PCollectionView<?>> sideInputs;

  private Requirements(Collection<PCollectionView<?>> sideInputs) {
    this.sideInputs = sideInputs;
  }

  /** The side inputs that this {@link Contextful} needs access to. */
  public Collection<PCollectionView<?>> getSideInputs() {
    return sideInputs;
  }

  /** Describes the need for access to the given side inputs. */
  public static Requirements requiresSideInputs(Collection<PCollectionView<?>> sideInputs) {
    return new Requirements(sideInputs);
  }

  /** Like {@link #requiresSideInputs(Collection)}. */
  public static Requirements requiresSideInputs(PCollectionView<?>... sideInputs) {
    return requiresSideInputs(Arrays.asList(sideInputs));
  }

  /** Describes an empty set of requirements. */
  public static Requirements empty() {
    return new Requirements(Collections.emptyList());
  }

  /** Whether this is an empty set of requirements. */
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  public static Requirements union(Contextful... contextfuls) {
    Set<PCollectionView<?>> sideInputs = Sets.newHashSet();
    for (Contextful c : contextfuls) {
      sideInputs.addAll(c.getRequirements().getSideInputs());
    }
    return requiresSideInputs(sideInputs);
  }
}
