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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.junit.jupiter.api.Tag;

/** A utility class for querying annotations. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class Annotations {

  /** Annotation predicates. */
  static class Predicates {

    static Predicate<Annotation> isAnnotationOfType(
        final Class<? extends Annotation> annotationType) {
      Preconditions.checkNotNull(annotationType, "annotationType cannot be null");
      return annotation ->
          annotation.annotationType() != null
              && annotation.annotationType().equals(annotation.annotationType());
    }

    static Predicate<Annotation> isJUnit5TagNamed(final String expectedTagName) {
      Preconditions.checkNotNull(expectedTagName, "expectedTagName cannot be null");
      return annotation -> {
        if (annotation != null && Tag.class.equals(annotation.annotationType())) {
          // Cast is safe due to the preceding check.
          // Tag.value() is specified as @NonNull (by contract, though not explicitly by
          // @javax.annotation.Nonnull),
          // so no null check needed for its return value when comparing.
          return expectedTagName.equals(((Tag) annotation).value());
        }
        return false;
      };
    }
  }
}
