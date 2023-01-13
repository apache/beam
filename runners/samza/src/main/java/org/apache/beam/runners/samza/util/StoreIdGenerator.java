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
package org.apache.beam.runners.samza.util;

import java.util.Set;

/**
 * This class encapsulates the logic to generate unique store id. For unique state ids across the
 * Beam pipeline, store id is the same as the state id. For non-unique state ids, join the state id
 * with an escaped transform name to generate a unique store id.
 */
public class StoreIdGenerator {

  private final Set<String> nonUniqueStateIds;

  public StoreIdGenerator(Set<String> nonUniqueStateId) {
    this.nonUniqueStateIds = nonUniqueStateId;
  }

  public String getId(String stateId, String transformFullName) {
    String storeId = stateId;
    if (nonUniqueStateIds.contains(stateId)) {
      final String escapedName = SamzaPipelineTranslatorUtils.escape(transformFullName);
      storeId = toUniqueStoreId(stateId, escapedName);
    }
    return storeId;
  }

  /** Join state id and escaped PTransform name to uniquify store id. */
  private static String toUniqueStoreId(String stateId, String escapedPTransformName) {
    return String.join("-", stateId, escapedPTransformName);
  }
}
