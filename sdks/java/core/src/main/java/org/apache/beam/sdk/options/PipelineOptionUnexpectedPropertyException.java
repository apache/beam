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

import com.google.common.collect.Iterables;
import java.util.SortedSet;

/** An exception used when encounter unexpected properties. */
public class PipelineOptionUnexpectedPropertyException extends IllegalArgumentException {
  private static final long serialVersionUID = 3265630128856068164L;

  private final String propertyName;
  private final SortedSet<String> closestMatches;

  public static PipelineOptionUnexpectedPropertyException create(
      Class optionClass, String propertyName, SortedSet<String> closestMatches) {
    String errorMessage = createErrorMessage(optionClass, propertyName, closestMatches);
    PipelineOptionUnexpectedPropertyException exception =
        new PipelineOptionUnexpectedPropertyException(errorMessage, propertyName, closestMatches);
    return exception;
  }

  private static String createErrorMessage(
      Class optionClass, String propertyName, SortedSet<String> closestMatches) {
    String ret;

    switch (closestMatches.size()) {
      case 0:
        ret = String.format("Class %s missing a property named '%s'.", optionClass, propertyName);
        break;
      case 1:
        ret =
            String.format(
                "Class %s missing a property named '%s'. Did you mean '%s'?",
                optionClass, propertyName, Iterables.getOnlyElement(closestMatches));
        break;
      default:
        ret =
            String.format(
                "Class %s missing a property named '%s'. Did you mean one of %s?",
                optionClass, propertyName, closestMatches);
        break;
    }

    return ret;
  }

  private PipelineOptionUnexpectedPropertyException(
      String errorMessage, String propertyName, SortedSet<String> closestMatches) {
    super(errorMessage);
    this.propertyName = propertyName;
    this.closestMatches = closestMatches;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public SortedSet<String> getClosestMatches() {
    return closestMatches;
  }
}
