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
package org.apache.beam.runners.dataflow.worker;

import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.WorkItem;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class that knows how to build System to DFE PCollection Name mapping from WorkItem. */
public class SystemIdToDfePCollectionNameMappingBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(BatchDataflowWorker.class);

  /** @return System to DFE PCollection name mapping. */
  public Map<String, String> build(WorkItem workItem) {
    Map<String, String> pcollectionDfeSystemToNameMapping = new HashMap<>();
    for (ParallelInstruction instruction : workItem.getMapTask().getInstructions()) {
      if (instruction.getOutputs() == null) {
        continue;
      }
      for (InstructionOutput output : instruction.getOutputs()) {
        if (pcollectionDfeSystemToNameMapping.containsKey(output.getSystemName())) {
          LOG.warn("Found multiple output mappings for pcollectionKey", output.getSystemName());
        } else {
          // throw new RuntimeException(workItem.toString());
          // DFE prepends system name with "<instructionOriginalName>."
          final String trimmedSystemName =
              output.getSystemName().substring(instruction.getOriginalName().length() + 1);
          pcollectionDfeSystemToNameMapping.put(trimmedSystemName, output.getName());
        }
      }
    }
    return pcollectionDfeSystemToNameMapping;
  }
}
