/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.options;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link Validation} represents a set of annotations which can be used to annotate getter
 * properties on {@link PipelineOptions} with information representing the validation criteria to
 * be used when validating with the {@link PipelineOptionsValidator}.
 */

public @interface Validation {
  /**
   * This criteria specifies that the value must be not null. Note that this annotation
   * should only be applied to methods which return nullable objects.
   */
  @Target(value = ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Required {
  }
}
