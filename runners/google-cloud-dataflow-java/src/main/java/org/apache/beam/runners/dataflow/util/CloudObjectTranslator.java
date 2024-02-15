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
package org.apache.beam.runners.dataflow.util;

import org.apache.beam.sdk.util.construction.SdkComponents;

/**
 * A translator that takes an object and creates a {@link CloudObject} which can be converted back
 * to the original object.
 */
public interface CloudObjectTranslator<T> {
  /** Converts the provided object into an equivalent {@link CloudObject}. */
  CloudObject toCloudObject(T target, SdkComponents sdkComponents);

  /** Converts back into the original object from a provided {@link CloudObject}. */
  T fromCloudObject(CloudObject cloudObject);

  /** Gets the class this {@link CloudObjectTranslator} is capable of converting. */
  Class<? extends T> getSupportedClass();

  /**
   * Gets the class name that will represent the {@link CloudObject} created by this {@link
   * CloudObjectTranslator}.
   */
  String cloudObjectClassName();
}
