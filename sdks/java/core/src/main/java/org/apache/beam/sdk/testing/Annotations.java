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

import java.lang.annotation.Annotation;
import java.util.Arrays;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.junit.experimental.categories.Category;

/** A utility class for querying annotations. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class Annotations {

  /** Annotation predicates. */
  static class Predicates {

    static Predicate<Annotation> isAnnotationOfType(final Class<? extends Annotation> clazz) {
      return annotation ->
          annotation.annotationType() != null && annotation.annotationType().equals(clazz);
    }

    static Predicate<Annotation> isCategoryOf(final Class<?> value, final boolean allowDerived) {
      return category ->
          FluentIterable.from(Arrays.asList(((Category) category).value()))
              .anyMatch(
                  aClass -> allowDerived ? value.isAssignableFrom(aClass) : value.equals(aClass));
    }
  }
}
