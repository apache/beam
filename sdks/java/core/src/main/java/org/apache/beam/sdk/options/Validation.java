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
package org.apache.beam.sdk.options;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link Validation} represents a set of annotations that can be used to annotate getter properties
 * on {@link PipelineOptions} with information representing the validation criteria to be used when
 * validating with the {@link PipelineOptionsValidator}.
 */
public @interface Validation {
  /**
   * This criteria specifies that the value must be not null. Note that this annotation should only
   * be applied to methods that return nullable objects.
   */
  @Target(value = ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  @interface Required {
    /**
     * The groups that the annotated attribute is a member of. A member can be in 0 or more groups.
     * Members not in any groups are considered to be in a group consisting exclusively of
     * themselves. At least one member of a group must be non-null if the options are to be valid.
     */
    String[] groups() default {};
  }
}
