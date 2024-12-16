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
package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StateMultiplexingGroupByKeyTransformMatcher implements PTransformMatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateMultiplexingGroupByKeyTransformMatcher.class);
  private static final StateMultiplexingGroupByKeyTransformMatcher INSTANCE =
      new StateMultiplexingGroupByKeyTransformMatcher();

  @Override
  public boolean matches(AppliedPTransform<?, ?, ?> application) {
    LOG.info(application.getFullName());
    if (!(application.getTransform() instanceof GroupByKey)) {
      LOG.info(application.getFullName() + " returning false");
      return false;
    }
    for (PCollection<?> pCollection : application.getMainInputs().values()) {
      LOG.info(application.getFullName() + " " + pCollection.getCoder());
      Coder<?> coder = pCollection.getCoder();
      if (!(coder instanceof KvCoder)) {
        return false;
      }
      // Don't enable multiplexing on Void keys
      if (((KvCoder<?, ?>) coder).getKeyCoder() instanceof VoidCoder) {
        return false;
      }
    }
    LOG.info(application.getFullName() + " returning true");
    return true;
  }

  public static StateMultiplexingGroupByKeyTransformMatcher getInstance() {
    return INSTANCE;
  }
}
