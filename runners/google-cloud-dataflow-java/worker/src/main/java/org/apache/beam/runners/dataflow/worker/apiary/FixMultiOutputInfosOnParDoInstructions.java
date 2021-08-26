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
package org.apache.beam.runners.dataflow.worker.apiary;

import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * {@link ParDoInstruction}s are meant to always have {@link MultiOutputInfo}s which give names to
 * the outputs. This function fixes {@link ParDoInstruction}s by filling in a default {@link
 * MultiOutputInfo} with an output tag using the supplied id generator. Note that the id generator
 * should supply ids outside the ids used within the {@link MapTask} to prevent collisions.
 */
public class FixMultiOutputInfosOnParDoInstructions implements Function<MapTask, MapTask> {
  private final IdGenerator idGenerator;

  public FixMultiOutputInfosOnParDoInstructions(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  @Override
  public MapTask apply(MapTask input) {
    for (ParallelInstruction instruction : Apiary.listOrEmpty(input.getInstructions())) {
      ParDoInstruction parDoInstruction = instruction.getParDo();
      if (parDoInstruction != null) {
        int numOutputs = Apiary.intOrZero(parDoInstruction.getNumOutputs());
        List<MultiOutputInfo> multiOutputInfos =
            Apiary.listOrEmpty(parDoInstruction.getMultiOutputInfos());
        if (numOutputs != Apiary.listOrEmpty(instruction.getParDo().getMultiOutputInfos()).size()) {
          if (numOutputs == 1) {
            parDoInstruction.setMultiOutputInfos(
                ImmutableList.of(new MultiOutputInfo().setTag(idGenerator.getId())));
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "Invalid ParDoInstruction %s, %d outputs specified, found %s tags.",
                    instruction.getSystemName(), numOutputs, multiOutputInfos));
          }
        }
      }
    }
    return input;
  }
}
