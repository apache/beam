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
package org.apache.beam.runners.samza.translation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.samza.util.StateUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;

/**
 * This class identifies the set of non-unique state ids by scanning the BEAM {@link Pipeline} with
 * a topological traversal.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StateIdParser extends Pipeline.PipelineVisitor.Defaults {
  private final Set<String> nonUniqueStateIds = new HashSet<>();
  private final Set<String> usedStateIds = new HashSet<>();

  public static Set<String> scan(Pipeline pipeline) {
    final StateIdParser parser = new StateIdParser();
    pipeline.traverseTopologically(parser);
    return parser.getNonUniqueStateIds();
  }

  private StateIdParser() {}

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    if (node.getTransform() instanceof ParDo.MultiOutput) {
      final DoFn<?, ?> doFn = ((ParDo.MultiOutput) node.getTransform()).getFn();
      if (StateUtils.isStateful(doFn)) {
        final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
        for (String stateId : signature.stateDeclarations().keySet()) {
          if (!usedStateIds.add(stateId)) {
            nonUniqueStateIds.add(stateId);
          }
        }
      }
    }
  }

  public Set<String> getNonUniqueStateIds() {
    return Collections.unmodifiableSet(nonUniqueStateIds);
  }
}
