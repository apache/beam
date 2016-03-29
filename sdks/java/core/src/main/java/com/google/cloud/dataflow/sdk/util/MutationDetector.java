/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.util;

/**
 * An object for detecting illegal mutations.
 *
 * <p>The {@link AutoCloseable} aspect of this interface allows use in a try-with-resources
 * style, where the implementing class may choose to perform a final mutation check upon
 * {@link #close()}.
 */
public interface MutationDetector extends AutoCloseable {
  /**
   * @throws IllegalMutationException if illegal mutations are detected.
   */
  void verifyUnmodified();
}
